"""
Validation service for normalization.

This module provides comprehensive validation capabilities for normalized
market data including schema validation, business rules, and data quality checks.
"""

import asyncio
import json
import logging
from typing import Any, Dict, List, Optional, Union
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

import structlog
import yaml
from pydantic import BaseModel, Field, ValidationError


class ValidationResult(BaseModel):
    """Result of validation operation."""
    
    status: str = Field(..., description="Validation status")
    errors: List[str] = Field(default_factory=list, description="Validation errors")
    warnings: List[str] = Field(default_factory=list, description="Validation warnings")
    score: float = Field(..., description="Validation score")
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class ValidationRule(BaseModel):
    """Individual validation rule."""
    
    name: str = Field(..., description="Rule name")
    field: str = Field(..., description="Field to validate")
    rule_type: str = Field(..., description="Type of validation rule")
    parameters: Dict[str, Any] = Field(default_factory=dict, description="Rule parameters")
    severity: str = Field("error", description="Rule severity (error, warning, info)")
    enabled: bool = Field(True, description="Whether rule is enabled")


class ValidationService:
    """
    Validation service for market data.
    
    This service provides comprehensive validation capabilities including
    schema validation, business rules, and data quality checks.
    """
    
    def __init__(self, config_path: str = "configs/normalization_rules.yaml"):
        """
        Initialize the validation service.
        
        Args:
            config_path: Path to validation configuration
        """
        self.config_path = config_path
        self.logger = structlog.get_logger(__name__)
        
        # Load configuration
        self.config = self._load_config()
        
        # Validation rules
        self.rules: List[ValidationRule] = []
        self._compile_rules()
        
        # Validation statistics
        self.stats = {
            "total_validations": 0,
            "successful_validations": 0,
            "failed_validations": 0,
            "rule_validations": {},
            "error_counts": {}
        }
    
    def _load_config(self) -> Dict[str, Any]:
        """
        Load validation configuration.
        
        Returns:
            Dict[str, Any]: Configuration data
        """
        try:
            with open(self.config_path, 'r') as f:
                config = yaml.safe_load(f)
            self.logger.info("Validation configuration loaded", config_path=self.config_path)
            return config
        except Exception as e:
            self.logger.error("Failed to load validation configuration", 
                            error=str(e), config_path=self.config_path)
            return {}
    
    def _compile_rules(self) -> None:
        """Compile validation rules from configuration."""
        try:
            self.rules = []
            
            # Extract validation rules from configuration
            global_config = self.config.get("global", {})
            validation_config = global_config.get("validation", {})
            
            # Compile global validation rules
            if validation_config.get("strict_mode", False):
                self._add_strict_mode_rules()
            
            if validation_config.get("check_required_fields", True):
                self._add_required_field_rules()
            
            # Compile market-specific rules
            markets = self.config.get("markets", {})
            for market, market_config in markets.items():
                self._compile_market_rules(market, market_config)
            
            self.logger.info("Validation rules compiled successfully", rule_count=len(self.rules))
            
        except Exception as e:
            self.logger.error("Failed to compile validation rules", error=str(e))
            self.rules = []
    
    def _add_strict_mode_rules(self) -> None:
        """Add strict mode validation rules."""
        strict_rules = [
            ValidationRule(
                name="strict_no_null_values",
                field="*",
                rule_type="not_null",
                parameters={},
                severity="error"
            ),
            ValidationRule(
                name="strict_numeric_precision",
                field="*",
                rule_type="numeric_precision",
                parameters={"max_precision": 6},
                severity="warning"
            )
        ]
        self.rules.extend(strict_rules)
    
    def _add_required_field_rules(self) -> None:
        """Add required field validation rules."""
        required_fields = ["event_id", "occurred_at", "tenant_id", "schema_version", "producer"]
        
        for field in required_fields:
            rule = ValidationRule(
                name=f"required_{field}",
                field=field,
                rule_type="required",
                parameters={},
                severity="error"
            )
            self.rules.append(rule)
    
    def _compile_market_rules(self, market: str, market_config: Dict[str, Any]) -> None:
        """Compile market-specific validation rules."""
        # Add market-specific rules based on configuration
        if "field_mappings" in market_config:
            for old_field, new_field in market_config["field_mappings"].items():
                rule = ValidationRule(
                    name=f"{market}_mapped_field_{new_field}",
                    field=new_field,
                    rule_type="required",
                    parameters={},
                    severity="error"
                )
                self.rules.append(rule)
        
        # Add market-specific validation rules
        if "validation_rules" in market_config:
            for rule_config in market_config["validation_rules"]:
                rule = ValidationRule(
                    name=f"{market}_{rule_config.get('name', 'unknown')}",
                    field=rule_config.get("field", "*"),
                    rule_type=rule_config.get("type", "basic"),
                    parameters=rule_config.get("parameters", {}),
                    severity=rule_config.get("severity", "error")
                )
                self.rules.append(rule)
    
    async def validate(self, data: Dict[str, Any]) -> ValidationResult:
        """
        Validate normalized data.
        
        Args:
            data: Data to validate
            
        Returns:
            ValidationResult: Validation result
        """
        try:
            self.stats["total_validations"] += 1
            
            errors = []
            warnings = []
            
            # Apply validation rules
            for rule in self.rules:
                if not rule.enabled:
                    continue
                
                try:
                    rule_result = await self._apply_rule(rule, data)
                    
                    if rule_result["status"] == "error":
                        errors.extend(rule_result["messages"])
                    elif rule_result["status"] == "warning":
                        warnings.extend(rule_result["messages"])
                    
                    # Update statistics
                    rule_name = rule.name
                    self.stats["rule_validations"][rule_name] = (
                        self.stats["rule_validations"].get(rule_name, 0) + 1
                    )
                    
                    # Count errors by type
                    for message in rule_result["messages"]:
                        error_type = rule.rule_type
                        self.stats["error_counts"][error_type] = (
                            self.stats["error_counts"].get(error_type, 0) + 1
                        )
                
                except Exception as e:
                    self.logger.error("Failed to apply validation rule", 
                                    rule_name=rule.name, 
                                    error=str(e))
                    continue
            
            # Calculate validation score
            score = self._calculate_validation_score(data, errors, warnings)
            
            # Determine overall status
            if errors:
                status = "invalid"
                self.stats["failed_validations"] += 1
            elif warnings:
                status = "warning"
                self.stats["successful_validations"] += 1
            else:
                status = "valid"
                self.stats["successful_validations"] += 1
            
            result = ValidationResult(
                status=status,
                errors=errors,
                warnings=warnings,
                score=score
            )
            
            self.logger.info("Data validation completed", 
                           status=status,
                           error_count=len(errors),
                           warning_count=len(warnings),
                           score=score)
            
            return result
            
        except Exception as e:
            self.stats["failed_validations"] += 1
            self.logger.error("Failed to validate data", error=str(e))
            
            return ValidationResult(
                status="error",
                errors=[str(e)],
                warnings=[],
                score=0.0
            )
    
    async def _apply_rule(self, rule: ValidationRule, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Apply a single validation rule.
        
        Args:
            rule: Validation rule to apply
            data: Data to validate
            
        Returns:
            Dict[str, Any]: Rule application result
        """
        try:
            if rule.field == "*":
                # Apply to all fields
                messages = []
                for field_name, field_value in data.items():
                    field_messages = await self._validate_field(
                        field_name, field_value, rule.rule_type, rule.parameters
                    )
                    messages.extend(field_messages)
                
                return {
                    "status": "error" if messages else "success",
                    "messages": messages
                }
            else:
                # Apply to specific field
                field_value = data.get(rule.field)
                messages = await self._validate_field(
                    rule.field, field_value, rule.rule_type, rule.parameters
                )
                
                return {
                    "status": "error" if messages else "success",
                    "messages": messages
                }
                
        except Exception as e:
            self.logger.error("Failed to apply validation rule", 
                            rule_name=rule.name, 
                            error=str(e))
            return {
                "status": "error",
                "messages": [f"Rule application failed: {str(e)}"]
            }
    
    async def _validate_field(
        self, 
        field_name: str, 
        field_value: Any, 
        rule_type: str, 
        parameters: Dict[str, Any]
    ) -> List[str]:
        """
        Validate a single field.
        
        Args:
            field_name: Name of the field
            field_value: Value of the field
            rule_type: Type of validation rule
            parameters: Rule parameters
            
        Returns:
            List[str]: Validation messages
        """
        messages = []
        
        try:
            if rule_type == "required":
                if field_value is None or field_value == "":
                    messages.append(f"Required field '{field_name}' is missing or empty")
            
            elif rule_type == "not_null":
                if field_value is None:
                    messages.append(f"Field '{field_name}' cannot be null")
            
            elif rule_type == "numeric":
                if field_value is not None and not isinstance(field_value, (int, float)):
                    messages.append(f"Field '{field_name}' must be numeric")
            
            elif rule_type == "numeric_precision":
                if isinstance(field_value, (int, float)):
                    max_precision = parameters.get("max_precision", 6)
                    if len(str(field_value).split('.')[-1]) > max_precision:
                        messages.append(f"Field '{field_name}' exceeds maximum precision of {max_precision}")
            
            elif rule_type == "range":
                if isinstance(field_value, (int, float)):
                    min_val = parameters.get("min")
                    max_val = parameters.get("max")
                    
                    if min_val is not None and field_value < min_val:
                        messages.append(f"Field '{field_name}' is below minimum value {min_val}")
                    
                    if max_val is not None and field_value > max_val:
                        messages.append(f"Field '{field_name}' is above maximum value {max_val}")
            
            elif rule_type == "enum":
                allowed_values = parameters.get("values", [])
                if field_value not in allowed_values:
                    messages.append(f"Field '{field_name}' must be one of {allowed_values}")
            
            elif rule_type == "regex":
                pattern = parameters.get("pattern", "")
                if pattern and not self._matches_regex(str(field_value), pattern):
                    messages.append(f"Field '{field_name}' does not match pattern {pattern}")
            
            elif rule_type == "length":
                if isinstance(field_value, str):
                    min_length = parameters.get("min_length")
                    max_length = parameters.get("max_length")
                    
                    if min_length is not None and len(field_value) < min_length:
                        messages.append(f"Field '{field_name}' is too short (minimum {min_length})")
                    
                    if max_length is not None and len(field_value) > max_length:
                        messages.append(f"Field '{field_name}' is too long (maximum {max_length})")
            
            elif rule_type == "date_format":
                date_format = parameters.get("format", "%Y-%m-%d")
                if field_value and not self._is_valid_date(str(field_value), date_format):
                    messages.append(f"Field '{field_name}' is not a valid date in format {date_format}")
            
            elif rule_type == "email":
                if field_value and not self._is_valid_email(str(field_value)):
                    messages.append(f"Field '{field_name}' is not a valid email address")
            
            elif rule_type == "url":
                if field_value and not self._is_valid_url(str(field_value)):
                    messages.append(f"Field '{field_name}' is not a valid URL")
            
            else:
                self.logger.warning("Unknown validation rule type", rule_type=rule_type)
        
        except Exception as e:
            messages.append(f"Validation error for field '{field_name}': {str(e)}")
        
        return messages
    
    def _matches_regex(self, value: str, pattern: str) -> bool:
        """Check if value matches regex pattern."""
        try:
            import re
            return bool(re.match(pattern, value))
        except Exception:
            return False
    
    def _is_valid_date(self, value: str, date_format: str) -> bool:
        """Check if value is a valid date."""
        try:
            datetime.strptime(value, date_format)
            return True
        except ValueError:
            return False
    
    def _is_valid_email(self, value: str) -> bool:
        """Check if value is a valid email address."""
        try:
            import re
            pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
            return bool(re.match(pattern, value))
        except Exception:
            return False
    
    def _is_valid_url(self, value: str) -> bool:
        """Check if value is a valid URL."""
        try:
            import re
            pattern = r'^https?://[^\s/$.?#].[^\s]*$'
            return bool(re.match(pattern, value))
        except Exception:
            return False
    
    def _calculate_validation_score(
        self, 
        data: Dict[str, Any], 
        errors: List[str], 
        warnings: List[str]
    ) -> float:
        """
        Calculate validation score.
        
        Args:
            data: Data that was validated
            errors: List of validation errors
            warnings: List of validation warnings
            
        Returns:
            float: Validation score between 0.0 and 1.0
        """
        score = 1.0
        
        # Deduct for errors
        score -= len(errors) * 0.1
        
        # Deduct for warnings
        score -= len(warnings) * 0.05
        
        # Deduct for missing fields
        required_fields = ["event_id", "occurred_at", "tenant_id", "schema_version", "producer"]
        missing_fields = [field for field in required_fields if field not in data or data[field] is None]
        score -= len(missing_fields) * 0.1
        
        # Deduct for null values
        null_count = sum(1 for v in data.values() if v is None)
        total_fields = len(data)
        if total_fields > 0:
            score -= (null_count / total_fields) * 0.2
        
        return max(0.0, min(1.0, score))
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get validation statistics.
        
        Returns:
            Dict[str, Any]: Validation statistics
        """
        return {
            **self.stats,
            "total_rules": len(self.rules),
            "enabled_rules": len([r for r in self.rules if r.enabled]),
            "success_rate": (
                self.stats["successful_validations"] / max(1, self.stats["total_validations"])
            )
        }
    
    def reset_stats(self) -> None:
        """Reset validation statistics."""
        self.stats = {
            "total_validations": 0,
            "successful_validations": 0,
            "failed_validations": 0,
            "rule_validations": {},
            "error_counts": {}
        }
    
    async def get_health_status(self) -> Dict[str, Any]:
        """Get health status of the validation service."""
        return {
            "status": "healthy",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "uptime_seconds": 0,
            "processing_stats": self.get_stats(),
            "validation_stats": self.get_stats(),
            "errors": []
        }
    
    async def is_ready(self) -> bool:
        """Check if the validation service is ready."""
        return True
    
    async def get_metrics(self) -> Dict[str, Any]:
        """Get validation service metrics."""
        stats = self.get_stats()
        stats["uptime_seconds"] = 0  # Would need to track start time
        return stats
    
    async def get_error_metrics(self) -> Dict[str, Any]:
        """Get error metrics."""
        error_types = self.stats["error_counts"].copy()
        if "validation" not in error_types:
            error_types["validation"] = self.stats["failed_validations"]
        
        return {
            "total_errors": self.stats["failed_validations"],
            "validation_errors": self.stats["failed_validations"],
            "transformation_errors": 0,
            "error_rate": self.stats["failed_validations"] / max(1, self.stats["total_validations"]),
            "error_types": error_types
        }