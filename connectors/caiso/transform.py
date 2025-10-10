"""
CAISO data transformation implementation.

This module provides the CAISO transformer for converting raw CAISO data
into the standardized format expected by the ingestion platform.
"""

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from ..base import ExtractionResult, TransformationResult
from ..base.exceptions import TransformationError
from ..base.utils import setup_logging


class CAISOTransformConfig(BaseModel):
    """CAISO transformation configuration."""
    
    timezone: str = Field("UTC", description="Target timezone for timestamps")
    decimal_precision: int = Field(6, description="Decimal precision for prices")
    validate_required_fields: bool = Field(True, description="Validate required fields")
    sanitize_numeric: bool = Field(True, description="Sanitize numeric values")
    standardize_timezone: bool = Field(True, description="Standardize timezone")


class CAISOTransform:
    """CAISO data transformer."""
    
    def __init__(
        self,
        config: Optional[CAISOTransformConfig] = None,
        *,
        raise_on_validation_error: bool = True,
    ):
        """
        Initialize the CAISO transformer.
        
        Args:
            config: Transformation configuration
        """
        if config is None:
            self.config = CAISOTransformConfig()
        elif isinstance(config, CAISOTransformConfig):
            self.config = config
        else:
            raw_config: Dict[str, Any]
            if hasattr(config, "dict"):
                raw_config = dict(config.dict())  # type: ignore[attr-defined]
            elif isinstance(config, dict):
                raw_config = dict(config)
            else:
                raw_config = {}
            model_fields = getattr(CAISOTransformConfig, "model_fields", None)
            if model_fields is not None:
                allowed_keys = model_fields.keys()
            else:
                allowed_keys = CAISOTransformConfig.__fields__.keys()  # type: ignore[attr-defined]
            filtered = {k: raw_config[k] for k in allowed_keys if k in raw_config}
            self.config = CAISOTransformConfig(**filtered)
        self.raise_on_validation_error = raise_on_validation_error
        self.logger = setup_logging(self.__class__.__name__)
    
    def sanitize_numeric(self, value: Any, field_name: str) -> Optional[float]:
        """
        Sanitize numeric values.
        
        Args:
            value: Value to sanitize
            field_name: Field name for logging
            
        Returns:
            Optional[float]: Sanitized numeric value
        """
        if value is None:
            return None
        
        try:
            if isinstance(value, str):
                # Remove common non-numeric characters
                cleaned = value.replace(",", "").replace("$", "").strip()
                if cleaned == "" or cleaned.upper() == "NULL":
                    return None
                return float(cleaned)
            elif isinstance(value, (int, float)):
                return float(value)
            else:
                self.logger.warning(f"Unexpected type for {field_name}: {type(value)}")
                return None
        except (ValueError, TypeError) as e:
            self.logger.warning(f"Failed to sanitize {field_name}: {e}")
            return None
    
    def standardize_timezone(self, timestamp: int) -> int:
        """
        Standardize timestamp to UTC.
        
        Args:
            timestamp: Timestamp in microseconds
            
        Returns:
            int: Standardized timestamp in microseconds
        """
        try:
            # Convert microseconds to datetime
            dt = datetime.fromtimestamp(timestamp / 1_000_000, tz=timezone.utc)
            
            # Ensure it's in UTC
            if dt.tzinfo != timezone.utc:
                dt = dt.astimezone(timezone.utc)
            
            # Convert back to microseconds
            return int(dt.timestamp() * 1_000_000)
        except Exception as e:
            self.logger.warning(f"Failed to standardize timezone: {e}")
            return timestamp
    
    def validate_record(self, record: Dict[str, Any]) -> List[str]:
        """
        Validate a transformed record.
        
        Args:
            record: Record to validate
            
        Returns:
            List[str]: List of validation errors
        """
        errors = []
        
        if self.config.validate_required_fields:
            required_fields = ["event_id", "occurred_at", "tenant_id", "schema_version", "producer"]
            
            for field in required_fields:
                if field not in record or record[field] is None:
                    errors.append(f"Missing required field: {field}")
        
        # Validate timestamp
        if "occurred_at" in record and record["occurred_at"] is not None:
            try:
                timestamp = int(record["occurred_at"])
                if timestamp <= 0:
                    errors.append("Invalid timestamp: must be positive")
            except (ValueError, TypeError):
                errors.append("Invalid timestamp: must be integer")
        
        # Validate price fields
        price_fields = ["price", "bid_price", "offer_price", "clearing_price", "congestion_price", "loss_price"]
        for field in price_fields:
            if field in record and record[field] is not None:
                try:
                    price = float(record[field])
                    if price < 0:
                        errors.append(f"Invalid {field}: must be non-negative")
                except (ValueError, TypeError):
                    errors.append(f"Invalid {field}: must be numeric")
        
        # Validate quantity
        if "quantity" in record and record["quantity"] is not None:
            try:
                quantity = float(record["quantity"])
                if quantity < 0:
                    errors.append("Invalid quantity: must be non-negative")
            except (ValueError, TypeError):
                errors.append("Invalid quantity: must be numeric")
        
        return errors
    
    async def transform(self, extraction_result: ExtractionResult) -> TransformationResult:
        """
        Transform extracted CAISO data.
        
        Args:
            extraction_result: Result from the extract operation
            
        Returns:
            TransformationResult: Transformed data and metadata
        """
        try:
            transformed_data = []
            validation_errors = []
            
            for record in extraction_result.data:
                try:
                    # Start with the original record
                    transformed_record = record.copy()
                    
                    # Apply numeric sanitization
                    if self.config.sanitize_numeric:
                        numeric_fields = [
                            "price",
                            "quantity",
                            "bid_price",
                            "offer_price",
                            "clearing_price",
                            "congestion_price",
                            "loss_price",
                            "ghg_price",
                        ]

                        for field in numeric_fields:
                            if field in transformed_record:
                                transformed_record[field] = self.sanitize_numeric(
                                    transformed_record[field], field
                                )

                    # Apply timezone standardization
                    if (
                        self.config.standardize_timezone
                        and "occurred_at" in transformed_record
                        and transformed_record["occurred_at"] is not None
                    ):
                        transformed_record["occurred_at"] = self.standardize_timezone(
                            transformed_record["occurred_at"]
                        )
                    
                    # Add per-record transformation timestamp required by schema
                    transformed_record["transformation_timestamp"] = datetime.now(timezone.utc).isoformat()

                    # Validate the transformed record
                    record_errors = self.validate_record(transformed_record)
                    if record_errors:
                        validation_errors.extend([
                            f"Record {transformed_record.get('event_id', 'unknown')}: {error}"
                            for error in record_errors
                        ])
                    
                    transformed_data.append(transformed_record)
                    
                except Exception as e:
                    self.logger.error(f"Failed to transform record: {e}")
                    raise TransformationError(f"Transformation error: {e}") from e
            
            # Create transformation metadata
            transformation_metadata = {
                "transformation_time": datetime.now(timezone.utc).isoformat(),
                "config": self.config.dict(),
                "input_record_count": len(extraction_result.data),
                "output_record_count": len(transformed_data),
                "validation_errors_count": len(validation_errors),
                "transforms_applied": [
                    "sanitize_numeric",
                    "standardize_timezone"
                ]
            }
            
            # Remove None values from transforms_applied
            transformation_metadata["transforms_applied"] = [
                t for t in transformation_metadata["transforms_applied"] if t is not None
            ]

            if validation_errors and self.raise_on_validation_error:
                raise TransformationError("; ".join(validation_errors))
            
            return TransformationResult(
                data=transformed_data,
                metadata=transformation_metadata,
                record_count=len(transformed_data),
                validation_errors=validation_errors
            )
            
        except Exception as e:
            self.logger.error(f"Failed to transform CAISO data: {e}")
            raise TransformationError(f"CAISO data transformation failed: {e}") from e
    
    def get_transformation_stats(self) -> Dict[str, Any]:
        """
        Get transformation statistics.
        
        Returns:
            Dict[str, Any]: Transformation statistics
        """
        return {
            "config": self.config.dict(),
            "transforms_available": [
                "sanitize_numeric",
                "standardize_timezone"
            ],
            "validation_enabled": self.config.validate_required_fields,
            "timezone": self.config.timezone,
            "decimal_precision": self.config.decimal_precision
        }
