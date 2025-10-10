"""
Core normalization service implementation.

This module provides the main normalization logic for converting raw market data
into standardized, validated formats.
"""

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import uuid4

import structlog
import yaml
from pydantic import BaseModel, Field, ValidationError

from .rules_engine import RulesEngine
from .validators import ValidationService


class NormalizationResult(BaseModel):
    """Result of normalization operation."""
    
    normalized_data: Dict[str, Any] = Field(..., description="Normalized data")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Normalization metadata")
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    validation_status: str = Field(..., description="Validation status")
    validation_errors: List[str] = Field(default_factory=list, description="Validation errors")
    quality_score: float = Field(..., description="Data quality score")


class NormalizationService:
    """
    Core normalization service for market data.
    
    This service handles the normalization of raw market data into standardized
    formats with proper validation and quality scoring.
    """
    
    def __init__(self, config_path: str = "configs/normalization_rules.yaml"):
        """
        Initialize the normalization service.
        
        Args:
            config_path: Path to normalization rules configuration
        """
        self.config_path = config_path
        self.logger = structlog.get_logger(__name__)
        
        # Initialize components
        self.rules_engine = RulesEngine()
        self.validation_service = ValidationService()
        
        # Load configuration
        self.config = self._load_config()
        
        # Normalization statistics
        self.stats = {
            "total_processed": 0,
            "successful_normalizations": 0,
            "failed_normalizations": 0,
            "validation_errors": 0,
            "quality_scores": []
        }
    
    def _load_config(self) -> Dict[str, Any]:
        """
        Load normalization configuration.
        
        Returns:
            Dict[str, Any]: Configuration data
        """
        try:
            with open(self.config_path, 'r') as f:
                config = yaml.safe_load(f)
            self.logger.info("Normalization configuration loaded", config_path=self.config_path)
            return config
        except Exception as e:
            self.logger.error("Failed to load normalization configuration", 
                            error=str(e), config_path=self.config_path)
            return {}
    
    async def normalize(self, raw_data: Dict[str, Any]) -> NormalizationResult:
        """
        Normalize raw market data.
        
        Args:
            raw_data: Raw data to normalize
            
        Returns:
            NormalizationResult: Normalization result
        """
        try:
            self.stats["total_processed"] += 1
            
            # Extract market information
            market = self._extract_market(raw_data)
            if not market:
                raise ValueError("Unable to determine market from raw data")
            
            # Get market-specific rules
            market_rules = self.config.get("markets", {}).get(market, {})
            global_rules = self.config.get("global", {})
            
            # Apply normalization rules
            normalized_data = await self._apply_normalization_rules(
                raw_data, market_rules, global_rules
            )
            
            # Validate normalized data
            validation_result = await self.validation_service.validate(normalized_data)
            
            # Calculate quality score
            quality_score = self._calculate_quality_score(normalized_data, validation_result)
            
            # Create result
            result = NormalizationResult(
                normalized_data=normalized_data,
                metadata={
                    "market": market,
                    "market_rules_applied": list(market_rules.keys()),
                    "global_rules_applied": list(global_rules.keys()),
                    "normalization_timestamp": datetime.now(timezone.utc).isoformat(),
                    "source_data_keys": list(raw_data.keys())
                },
                validation_status=validation_result.status,
                validation_errors=validation_result.errors,
                quality_score=quality_score
            )
            
            # Update statistics
            if validation_result.status == "valid":
                self.stats["successful_normalizations"] += 1
            else:
                self.stats["failed_normalizations"] += 1
                self.stats["validation_errors"] += len(validation_result.errors)
            
            self.stats["quality_scores"].append(quality_score)
            
            self.logger.info("Data normalized successfully", 
                           market=market,
                           quality_score=quality_score,
                           validation_status=validation_result.status)
            
            return result
            
        except Exception as e:
            self.stats["failed_normalizations"] += 1
            self.logger.error("Failed to normalize data", error=str(e), raw_data=raw_data)
            
            # Return error result
            return NormalizationResult(
                normalized_data={},
                metadata={"error": str(e)},
                validation_status="error",
                validation_errors=[str(e)],
                quality_score=0.0
            )
    
    def _extract_market(self, raw_data: Dict[str, Any]) -> Optional[str]:
        """
        Extract market identifier from raw data.
        
        Args:
            raw_data: Raw data to analyze
            
        Returns:
            Optional[str]: Market identifier or None
        """
        # Try common market fields
        market_fields = ["market", "market_id", "iso", "exchange", "source"]
        
        for field in market_fields:
            if field in raw_data:
                market_value = raw_data[field]
                if isinstance(market_value, str):
                    return market_value.upper()
                elif market_value is not None:
                    return str(market_value).upper()
        
        # Try to infer from other fields
        if "miso" in str(raw_data).lower():
            return "MISO"
        elif "caiso" in str(raw_data).lower():
            return "CAISO"
        
        return None
    
    async def _apply_normalization_rules(
        self, 
        raw_data: Dict[str, Any], 
        market_rules: Dict[str, Any], 
        global_rules: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Apply normalization rules to raw data.
        
        Args:
            raw_data: Raw data to normalize
            market_rules: Market-specific rules
            global_rules: Global rules
            
        Returns:
            Dict[str, Any]: Normalized data
        """
        normalized_data = raw_data.copy()
        
        # Apply field mappings
        field_mappings = market_rules.get("field_mappings", {})
        for old_field, new_field in field_mappings.items():
            if old_field in normalized_data:
                normalized_data[new_field] = normalized_data.pop(old_field)
        
        # Apply transformations
        transforms = market_rules.get("transforms", [])
        for transform in transforms:
            normalized_data = await self._apply_transform(normalized_data, transform)
        
        # Apply global transformations
        global_transforms = global_rules.get("transformations", {})
        if global_transforms:
            normalized_data = await self._apply_global_transforms(normalized_data, global_transforms)
        
        # Add metadata
        normalized_data.update({
            "market_id": market_rules.get("market_id", "UNKNOWN"),
            "timezone": market_rules.get("timezone", global_rules.get("default_timezone", "UTC")),
            "currency": market_rules.get("currency", global_rules.get("default_currency", "USD")),
            "unit": market_rules.get("unit", global_rules.get("default_unit", "MWh")),
            "price_unit": market_rules.get("price_unit", global_rules.get("default_price_unit", "$/MWh")),
            "normalization_timestamp": datetime.now(timezone.utc).isoformat()
        })
        
        return normalized_data
    
    async def _apply_transform(self, data: Dict[str, Any], transform: Dict[str, Any]) -> Dict[str, Any]:
        """
        Apply a single transformation to data.
        
        Args:
            data: Data to transform
            transform: Transformation configuration
            
        Returns:
            Dict[str, Any]: Transformed data
        """
        transform_name = transform.get("name")
        
        if transform_name == "sanitize_numeric":
            return self._sanitize_numeric_fields(data, transform.get("fields", []))
        elif transform_name == "standardize_timezone":
            return self._standardize_timezone(data, transform.get("target_timezone", "UTC"))
        elif transform_name == "validate_required_fields":
            return self._validate_required_fields(data, transform.get("required_fields", []))
        else:
            self.logger.warning("Unknown transform", transform_name=transform_name)
            return data
    
    def _sanitize_numeric_fields(self, data: Dict[str, Any], fields: List[str]) -> Dict[str, Any]:
        """
        Sanitize numeric fields in data.
        
        Args:
            data: Data to sanitize
            fields: List of fields to sanitize
            
        Returns:
            Dict[str, Any]: Sanitized data
        """
        for field in fields:
            if field in data:
                try:
                    value = data[field]
                    if isinstance(value, str):
                        # Remove non-numeric characters except decimal point and minus
                        cleaned = ''.join(c for c in value if c.isdigit() or c in '.-')
                        if cleaned:
                            data[field] = float(cleaned)
                        else:
                            data[field] = None
                    elif isinstance(value, (int, float)):
                        data[field] = float(value)
                except (ValueError, TypeError):
                    data[field] = None
        
        return data
    
    def _standardize_timezone(self, data: Dict[str, Any], target_timezone: str) -> Dict[str, Any]:
        """
        Standardize timezone in data.
        
        Args:
            data: Data to standardize
            target_timezone: Target timezone
            
        Returns:
            Dict[str, Any]: Standardized data
        """
        # This is a simplified implementation
        # In production, you'd use proper timezone conversion libraries
        timezone_fields = ["delivery_datetime", "trade_datetime", "occurred_at"]
        
        for field in timezone_fields:
            if field in data:
                try:
                    # Convert to target timezone (simplified)
                    data[f"{field}_timezone"] = target_timezone
                except Exception as e:
                    self.logger.warning("Failed to standardize timezone", 
                                      field=field, error=str(e))
        
        return data
    
    def _validate_required_fields(self, data: Dict[str, Any], required_fields: List[str]) -> Dict[str, Any]:
        """
        Validate required fields in data.
        
        Args:
            data: Data to validate
            required_fields: List of required fields
            
        Returns:
            Dict[str, Any]: Validated data
        """
        missing_fields = []
        for field in required_fields:
            if field not in data or data[field] is None:
                missing_fields.append(field)
        
        if missing_fields:
            data["_validation_errors"] = data.get("_validation_errors", [])
            data["_validation_errors"].append(f"Missing required fields: {missing_fields}")
        
        return data
    
    async def _apply_global_transforms(self, data: Dict[str, Any], transforms: Dict[str, Any]) -> Dict[str, Any]:
        """
        Apply global transformations to data.
        
        Args:
            data: Data to transform
            transforms: Global transformation configuration
            
        Returns:
            Dict[str, Any]: Transformed data
        """
        # Apply numeric precision
        if "numeric_precision" in transforms:
            precision = transforms["numeric_precision"]
            for key, value in data.items():
                if isinstance(value, float):
                    data[key] = round(value, precision)
        
        # Apply string encoding
        if "string_encoding" in transforms:
            encoding = transforms["string_encoding"]
            for key, value in data.items():
                if isinstance(value, str):
                    try:
                        data[key] = value.encode(encoding).decode(encoding)
                    except (UnicodeEncodeError, UnicodeDecodeError):
                        pass
        
        return data
    
    def _calculate_completeness(self, data: Dict[str, Any]) -> float:
        """
        Calculate data completeness score.

        Args:
            data: Normalized data

        Returns:
            float: Completeness score between 0.0 and 1.0
        """
        # Define required fields for each data type
        required_fields = {
            "market_price": ["market", "delivery_location", "delivery_date", "delivery_hour", "price"],
            "trade": ["market", "delivery_location", "delivery_date", "delivery_hour", "price", "quantity"],
            "curve": ["market", "curve_type", "delivery_date", "delivery_hour", "price"],
            "system_status": ["market", "delivery_location", "timestamp", "status_type"],
            "lmp": ["market", "node", "timestamp", "lmp_usd_per_mwh"],
            "as": ["market", "timestamp", "product", "cleared_price_usd_per_mwh"],
            "pra": ["market", "auction", "planning_zone", "clearing_price_usd_per_mw_day"],
            "arr": ["market", "auction", "path", "clearing_price_usd_per_mw"],
            "tcr": ["market", "auction", "path", "clearing_price_usd_per_mw"],
        }

        data_type = data.get("data_type", "unknown")
        required = required_fields.get(data_type, ["market", "delivery_location", "price"])

        present_fields = sum(1 for field in required if field in data and data[field] is not None)
        return present_fields / len(required) if required else 1.0

    def _calculate_accuracy(self, data: Dict[str, Any]) -> float:
        """
        Calculate data accuracy score.

        Args:
            data: Normalized data

        Returns:
            float: Accuracy score between 0.0 and 1.0
        """
        score = 1.0

        # Check price ranges
        if "price" in data and data["price"] is not None:
            price = float(data["price"])
            if price < 0 or price > 1000:
                score -= 0.2
            elif price < 10 or price > 500:
                score -= 0.1

        # Check quantity ranges
        if "quantity" in data and data["quantity"] is not None:
            quantity = float(data["quantity"])
            if quantity < 0 or quantity > 10000:
                score -= 0.2
            elif quantity < 10 or quantity > 5000:
                score -= 0.1

        # Check timestamp validity
        if "occurred_at" in data and data["occurred_at"] is not None:
            try:
                timestamp = int(data["occurred_at"])
                if timestamp <= 0 or timestamp > int(datetime.now(timezone.utc).timestamp() * 1_000_000) * 2:
                    score -= 0.15
            except (ValueError, TypeError):
                score -= 0.15

        return max(0.0, score)

    def _calculate_consistency(self, data: Dict[str, Any]) -> float:
        """
        Calculate data consistency score.

        Args:
            data: Normalized data

        Returns:
            float: Consistency score between 0.0 and 1.0
        """
        score = 1.0

        # Check data type consistency
        data_type = data.get("data_type")
        if data_type:
            # Market-specific consistency checks
            market = data.get("market", "").upper()
            if market == "CAISO":
                if data_type == "market_price" and "clearing_price" not in data:
                    score -= 0.1
            elif market == "MISO":
                if data_type == "trade" and "bid_price" not in data:
                    score -= 0.1

        # Check field type consistency
        numeric_fields = ["price", "quantity", "bid_price", "offer_price", "clearing_price", "congestion_price", "loss_price"]
        for field in numeric_fields:
            if field in data and data[field] is not None:
                try:
                    float(data[field])
                except (ValueError, TypeError):
                    score -= 0.1

        # Check string field consistency
        string_fields = ["market", "delivery_location", "delivery_date", "currency", "unit"]
        for field in string_fields:
            if field in data and data[field] is not None and not isinstance(data[field], str):
                score -= 0.05

        return max(0.0, score)

    def _calculate_timeliness(self, data: Dict[str, Any]) -> float:
        """
        Calculate data timeliness score.

        Args:
            data: Normalized data

        Returns:
            float: Timeliness score between 0.0 and 1.0
        """
        if "occurred_at" not in data or data["occurred_at"] is None:
            return 0.5  # Neutral score if timestamp missing

        try:
            timestamp = int(data["occurred_at"])
            current_time = int(datetime.now(timezone.utc).timestamp() * 1_000_000)
            age_seconds = (current_time - timestamp) / 1_000_000

            # Score based on age
            if age_seconds < 3600:  # Less than 1 hour
                return 1.0
            elif age_seconds < 86400:  # Less than 1 day
                return 0.9
            elif age_seconds < 604800:  # Less than 1 week
                return 0.7
            elif age_seconds < 2592000:  # Less than 1 month
                return 0.5
            else:  # Older than 1 month
                return 0.3

        except (ValueError, TypeError):
            return 0.5  # Neutral score if timestamp invalid

    def _calculate_validity(self, data: Dict[str, Any]) -> float:
        """
        Calculate data validity score.

        Args:
            data: Normalized data

        Returns:
            float: Validity score between 0.0 and 1.0
        """
        score = 1.0

        # Check required envelope fields
        required_envelope = ["event_id", "occurred_at", "tenant_id", "schema_version", "producer"]
        for field in required_envelope:
            if field not in data or data[field] is None:
                score -= 0.2

        # Check market-specific required fields
        market = data.get("market", "").upper()
        if market == "CAISO":
            required_caiso = ["delivery_location", "delivery_date", "price"]
            for field in required_caiso:
                if field not in data or data[field] is None:
                    score -= 0.1
        elif market == "MISO":
            required_miso = ["delivery_location", "delivery_date", "price", "quantity"]
            for field in required_miso:
                if field not in data or data[field] is None:
                    score -= 0.1

        # Check data type validity
        data_type = data.get("data_type")
        if data_type and data_type not in ["market_price", "trade", "curve", "system_status", "lmp", "as", "pra", "arr", "tcr"]:
            score -= 0.1

        return max(0.0, score)

    def _calculate_quality_score(self, data: Dict[str, Any], validation_result) -> float:
        """
        Calculate overall data quality score.

        Args:
            data: Normalized data
            validation_result: Validation result

        Returns:
            float: Quality score between 0.0 and 1.0
        """
        # Calculate individual quality dimensions
        completeness = self._calculate_completeness(data)
        accuracy = self._calculate_accuracy(data)
        consistency = self._calculate_consistency(data)
        timeliness = self._calculate_timeliness(data)
        validity = self._calculate_validity(data)

        # Weighted average of quality dimensions
        weights = {
            "completeness": 0.25,
            "accuracy": 0.25,
            "consistency": 0.20,
            "timeliness": 0.15,
            "validity": 0.15
        }

        score = (
            completeness * weights["completeness"] +
            accuracy * weights["accuracy"] +
            consistency * weights["consistency"] +
            timeliness * weights["timeliness"] +
            validity * weights["validity"]
        )

        # Deduct for validation errors
        validation_errors = validation_result.errors
        score -= len(validation_errors) * 0.1

        # Deduct for missing fields
        missing_fields = data.get("_validation_errors", [])
        score -= len(missing_fields) * 0.05

        return max(0.0, min(1.0, score))
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get normalization statistics.
        
        Returns:
            Dict[str, Any]: Normalization statistics
        """
        if self.stats["quality_scores"]:
            avg_quality = sum(self.stats["quality_scores"]) / len(self.stats["quality_scores"])
        else:
            avg_quality = 0.0
        
        return {
            **self.stats,
            "average_quality_score": avg_quality,
            "success_rate": (
                self.stats["successful_normalizations"] / max(1, self.stats["total_processed"])
            )
        }
    
    def reset_stats(self) -> None:
        """Reset normalization statistics."""
        self.stats = {
            "total_processed": 0,
            "successful_normalizations": 0,
            "failed_normalizations": 0,
            "validation_errors": 0,
            "quality_scores": []
        }
    
    async def get_health_status(self) -> Dict[str, Any]:
        """Get health status of the normalization service."""
        return {
            "status": "healthy",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "uptime_seconds": 0,  # Would need to track start time
            "processing_stats": self.get_stats(),
            "errors": []
        }
    
    async def is_ready(self) -> bool:
        """Check if the service is ready to process data."""
        return True  # Service is always ready
    
    async def normalize_record(self, raw_record: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a single record."""
        try:
            result = await self.normalize(raw_record)
            return result.normalized_data
        except Exception as e:
            self.logger.error(f"Failed to normalize record: {e}")
            return {}
    
    async def normalize_batch(self, raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Normalize a batch of records."""
        normalized_records = []
        for record in raw_records:
            try:
                result = await self.normalize(record)
                normalized_records.append(result.normalized_data)
            except Exception as e:
                # Skip failed records in batch processing
                continue
        return normalized_records
    
    def _standardize_timestamp(self, timestamp: Any) -> int:
        """Standardize timestamp to microseconds since epoch."""
        if isinstance(timestamp, int):
            return timestamp
        elif isinstance(timestamp, str):
            try:
                dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                return int(dt.timestamp() * 1_000_000)
            except:
                return int(datetime.now(timezone.utc).timestamp() * 1_000_000)
        elif isinstance(timestamp, datetime):
            return int(timestamp.timestamp() * 1_000_000)
        else:
            return int(datetime.now(timezone.utc).timestamp() * 1_000_000)
    
    async def _standardize_instrument_fields(self, data: Dict[str, Any], market: str) -> Dict[str, Any]:
        """Standardize instrument fields."""
        result = data.copy()
        result["instrument_id"] = f"{market}_{data.get('instrument_id', 'UNKNOWN')}"
        result["instrument_type"] = "Energy"
        result["delivery_location"] = data.get("delivery_location", "UNKNOWN")
        result["delivery_date"] = data.get("delivery_date", "")
        result["delivery_hour"] = data.get("delivery_hour", 0)
        return result
    
    async def _standardize_price_fields(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Standardize price fields."""
        result = data.copy()
        # Convert price to float if possible
        if "price" in result and result["price"] is not None:
            try:
                result["price"] = float(result["price"])
            except (ValueError, TypeError):
                result["price"] = None
        
        # Convert quantity to float if possible
        if "quantity" in result and result["quantity"] is not None:
            try:
                result["quantity"] = float(result["quantity"])
            except (ValueError, TypeError):
                result["quantity"] = None
        
        return result
    
    def _generate_quality_flags(self, data: Dict[str, Any]) -> List[str]:
        """Generate quality flags for the data."""
        flags = []
        
        # Check for missing required fields
        required_fields = ["event_id", "occurred_at", "price"]
        for field in required_fields:
            if field not in data or data[field] is None:
                flags.append(f"missing_{field}")
        
        # Check for invalid data types
        if "price" in data and data["price"] is not None:
            if not isinstance(data["price"], (int, float)):
                flags.append("invalid_price_type")
        
        return flags
    
    async def get_processing_metrics(self) -> Dict[str, Any]:
        """Get processing metrics."""
        stats = self.get_stats()
        return {
            "uptime_seconds": 0,  # Would need to track start time
            "total_records": stats["total_processed"],
            "successful_records": stats["successful_normalizations"],
            "failed_records": stats["failed_normalizations"],
            "success_rate": stats["success_rate"],
            "error_rate": 1.0 - stats["success_rate"],
            "throughput_per_second": 0  # Would need to calculate
        }
    
    async def get_throughput_metrics(self) -> Dict[str, Any]:
        """Get throughput metrics."""
        stats = self.get_stats()
        return {
            "records_per_second": 0,  # Would need to calculate
            "successful_per_second": 0,  # Would need to calculate
            "failed_per_second": 0,  # Would need to calculate
            "total_records": stats["total_processed"],
            "uptime_seconds": 0  # Would need to track start time
        }
    
    async def get_latency_metrics(self) -> Dict[str, Any]:
        """Get latency metrics."""
        processing_times = getattr(self.stats, "processing_times", [])
        if not processing_times:
            return {
                "p50_latency_ms": 0,
                "p95_latency_ms": 0,
                "p99_latency_ms": 0,
                "max_latency_ms": 0,
                "avg_latency_ms": 0
            }
        
        sorted_times = sorted(processing_times)
        n = len(sorted_times)
        
        return {
            "p50_latency_ms": sorted_times[int(n * 0.5)] * 1000,
            "p95_latency_ms": sorted_times[int(n * 0.95)] * 1000,
            "p99_latency_ms": sorted_times[int(n * 0.99)] * 1000,
            "max_latency_ms": max(sorted_times) * 1000,
            "avg_latency_ms": sum(sorted_times) / n * 1000
        }
    
    async def get_error_metrics(self) -> Dict[str, Any]:
        """Get error metrics."""
        errors = getattr(self.stats, "errors", [])
        error_types = {}
        for error in errors:
            error_type = type(error).__name__ if hasattr(error, '__class__') else "Unknown"
            error_types[error_type] = error_types.get(error_type, 0) + 1
        
        return {
            "total_errors": len(errors),
            "validation_errors": len([e for e in errors if "validation" in str(e).lower()]),
            "transformation_errors": len([e for e in errors if "transformation" in str(e).lower()]),
            "error_rate": len(errors) / max(1, self.stats["total_processed"]),
            "error_types": error_types
        }
