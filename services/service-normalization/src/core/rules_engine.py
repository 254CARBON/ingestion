"""
Rules engine for normalization service.

This module provides a flexible rules engine for applying normalization
rules to market data based on configuration.
"""

import asyncio
import json
import logging
from typing import Any, Dict, List, Optional, Union
from datetime import datetime, timezone

import structlog
import yaml
from pydantic import BaseModel, Field


class Rule(BaseModel):
    """Individual normalization rule."""
    
    name: str = Field(..., description="Rule name")
    condition: Optional[Dict[str, Any]] = Field(None, description="Rule condition")
    action: Dict[str, Any] = Field(..., description="Rule action")
    priority: int = Field(0, description="Rule priority")
    enabled: bool = Field(True, description="Whether rule is enabled")


class RulesEngine:
    """
    Rules engine for applying normalization rules.
    
    This engine provides a flexible way to apply normalization rules
    to market data based on configuration.
    """
    
    def __init__(self, config_path: str = "configs/normalization_rules.yaml"):
        """
        Initialize the rules engine.
        
        Args:
            config_path: Path to rules configuration
        """
        self.config_path = config_path
        self.logger = structlog.get_logger(__name__)
        
        # Load rules configuration
        self.config = self._load_config()
        
        # Compiled rules
        self.rules: List[Rule] = []
        self._compile_rules()
        
        # Execution statistics
        self.stats = {
            "total_executions": 0,
            "successful_executions": 0,
            "failed_executions": 0,
            "rule_executions": {}
        }
    
    def _load_config(self) -> Dict[str, Any]:
        """
        Load rules configuration.
        
        Returns:
            Dict[str, Any]: Configuration data
        """
        try:
            with open(self.config_path, 'r') as f:
                config = yaml.safe_load(f)
            self.logger.info("Rules configuration loaded", config_path=self.config_path)
            return config
        except Exception as e:
            self.logger.error("Failed to load rules configuration", 
                            error=str(e), config_path=self.config_path)
            return {}
    
    def _compile_rules(self) -> None:
        """Compile rules from configuration."""
        try:
            self.rules = []
            
            # Extract rules from configuration
            markets = self.config.get("markets", {})
            global_rules = self.config.get("global", {})
            
            # Compile market-specific rules
            for market, market_config in markets.items():
                transforms = market_config.get("transforms", [])
                for transform in transforms:
                    rule = Rule(
                        name=f"{market}_{transform.get('name', 'unknown')}",
                        condition={"market": market},
                        action=transform,
                        priority=1
                    )
                    self.rules.append(rule)
            
            # Compile global rules
            global_transforms = global_rules.get("transformations", {})
            if global_transforms:
                rule = Rule(
                    name="global_transforms",
                    condition={},
                    action=global_transforms,
                    priority=0
                )
                self.rules.append(rule)
            
            # Sort rules by priority
            self.rules.sort(key=lambda r: r.priority, reverse=True)
            
            self.logger.info("Rules compiled successfully", rule_count=len(self.rules))
            
        except Exception as e:
            self.logger.error("Failed to compile rules", error=str(e))
            self.rules = []
    
    async def apply_rules(self, data: Dict[str, Any], context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Apply rules to data.
        
        Args:
            data: Data to apply rules to
            context: Additional context for rule evaluation
            
        Returns:
            Dict[str, Any]: Data after applying rules
        """
        try:
            self.stats["total_executions"] += 1
            
            result_data = data.copy()
            context = context or {}
            
            # Apply each rule
            for rule in self.rules:
                if not rule.enabled:
                    continue
                
                try:
                    # Check rule condition
                    if self._evaluate_condition(rule.condition, result_data, context):
                        # Apply rule action
                        result_data = await self._apply_action(rule.action, result_data, context)
                        
                        # Update statistics
                        rule_name = rule.name
                        self.stats["rule_executions"][rule_name] = (
                            self.stats["rule_executions"].get(rule_name, 0) + 1
                        )
                        
                        self.logger.debug("Rule applied successfully", 
                                        rule_name=rule_name,
                                        data_keys=list(result_data.keys()))
                
                except Exception as e:
                    self.logger.error("Failed to apply rule", 
                                    rule_name=rule.name, 
                                    error=str(e))
                    continue
            
            self.stats["successful_executions"] += 1
            return result_data
            
        except Exception as e:
            self.stats["failed_executions"] += 1
            self.logger.error("Failed to apply rules", error=str(e))
            return data
    
    def _evaluate_condition(
        self, 
        condition: Optional[Dict[str, Any]], 
        data: Dict[str, Any], 
        context: Dict[str, Any]
    ) -> bool:
        """
        Evaluate rule condition.
        
        Args:
            condition: Condition to evaluate
            data: Data to evaluate against
            context: Additional context
            
        Returns:
            bool: True if condition is met, False otherwise
        """
        if not condition:
            return True
        
        try:
            # Simple condition evaluation
            for key, expected_value in condition.items():
                actual_value = data.get(key) or context.get(key)
                
                if isinstance(expected_value, dict):
                    # Complex condition
                    if not self._evaluate_complex_condition(expected_value, actual_value):
                        return False
                else:
                    # Simple equality check
                    if actual_value != expected_value:
                        return False
            
            return True
            
        except Exception as e:
            self.logger.error("Failed to evaluate condition", 
                            condition=condition, 
                            error=str(e))
            return False
    
    def _evaluate_complex_condition(self, condition: Dict[str, Any], value: Any) -> bool:
        """
        Evaluate complex condition.
        
        Args:
            condition: Complex condition
            value: Value to evaluate
            
        Returns:
            bool: True if condition is met, False otherwise
        """
        try:
            operator = condition.get("operator", "eq")
            
            if operator == "eq":
                return value == condition.get("value")
            elif operator == "ne":
                return value != condition.get("value")
            elif operator == "gt":
                return value > condition.get("value")
            elif operator == "gte":
                return value >= condition.get("value")
            elif operator == "lt":
                return value < condition.get("value")
            elif operator == "lte":
                return value <= condition.get("value")
            elif operator == "in":
                return value in condition.get("value", [])
            elif operator == "nin":
                return value not in condition.get("value", [])
            elif operator == "contains":
                return condition.get("value") in str(value)
            elif operator == "regex":
                import re
                pattern = condition.get("value", "")
                return bool(re.match(pattern, str(value)))
            else:
                self.logger.warning("Unknown operator", operator=operator)
                return False
                
        except Exception as e:
            self.logger.error("Failed to evaluate complex condition", 
                            condition=condition, 
                            error=str(e))
            return False
    
    async def _apply_action(
        self, 
        action: Dict[str, Any], 
        data: Dict[str, Any], 
        context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Apply rule action.
        
        Args:
            action: Action to apply
            data: Data to apply action to
            context: Additional context
            
        Returns:
            Dict[str, Any]: Data after applying action
        """
        try:
            action_type = action.get("type", "transform")
            
            if action_type == "transform":
                return await self._apply_transform_action(action, data, context)
            elif action_type == "validate":
                return await self._apply_validate_action(action, data, context)
            elif action_type == "enrich":
                return await self._apply_enrich_action(action, data, context)
            else:
                self.logger.warning("Unknown action type", action_type=action_type)
                return data
                
        except Exception as e:
            self.logger.error("Failed to apply action", 
                            action=action, 
                            error=str(e))
            return data
    
    async def _apply_transform_action(
        self, 
        action: Dict[str, Any], 
        data: Dict[str, Any], 
        context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Apply transform action.
        
        Args:
            action: Transform action
            data: Data to transform
            context: Additional context
            
        Returns:
            Dict[str, Any]: Transformed data
        """
        transform_name = action.get("name")
        
        if transform_name == "sanitize_numeric":
            return self._sanitize_numeric_fields(data, action.get("fields", []))
        elif transform_name == "standardize_timezone":
            return self._standardize_timezone(data, action.get("target_timezone", "UTC"))
        elif transform_name == "validate_required_fields":
            return self._validate_required_fields(data, action.get("required_fields", []))
        elif transform_name == "convert_units":
            return self._convert_units(data, action.get("conversions", {}))
        elif transform_name == "add_metadata":
            return self._add_metadata(data, action.get("metadata", {}))
        else:
            self.logger.warning("Unknown transform", transform_name=transform_name)
            return data
    
    async def _apply_validate_action(
        self, 
        action: Dict[str, Any], 
        data: Dict[str, Any], 
        context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Apply validate action.
        
        Args:
            action: Validate action
            data: Data to validate
            context: Additional context
            
        Returns:
            Dict[str, Any]: Validated data
        """
        validation_type = action.get("validation_type", "basic")
        
        if validation_type == "basic":
            return self._basic_validation(data, action.get("rules", {}))
        elif validation_type == "schema":
            return self._schema_validation(data, action.get("schema", {}))
        else:
            self.logger.warning("Unknown validation type", validation_type=validation_type)
            return data
    
    async def _apply_enrich_action(
        self, 
        action: Dict[str, Any], 
        data: Dict[str, Any], 
        context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Apply enrich action.
        
        Args:
            action: Enrich action
            data: Data to enrich
            context: Additional context
            
        Returns:
            Dict[str, Any]: Enriched data
        """
        enrichment_type = action.get("enrichment_type", "basic")
        
        if enrichment_type == "basic":
            return self._basic_enrichment(data, action.get("enrichments", {}))
        elif enrichment_type == "geospatial":
            return self._geospatial_enrichment(data, action.get("geospatial_config", {}))
        else:
            self.logger.warning("Unknown enrichment type", enrichment_type=enrichment_type)
            return data
    
    def _sanitize_numeric_fields(self, data: Dict[str, Any], fields: List[str]) -> Dict[str, Any]:
        """Sanitize numeric fields in data."""
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
        """Standardize timezone in data."""
        timezone_fields = ["delivery_datetime", "trade_datetime", "occurred_at"]
        
        for field in timezone_fields:
            if field in data:
                try:
                    data[f"{field}_timezone"] = target_timezone
                except Exception as e:
                    self.logger.warning("Failed to standardize timezone", 
                                      field=field, error=str(e))
        
        return data
    
    def _validate_required_fields(self, data: Dict[str, Any], required_fields: List[str]) -> Dict[str, Any]:
        """Validate required fields in data."""
        missing_fields = []
        for field in required_fields:
            if field not in data or data[field] is None:
                missing_fields.append(field)
        
        if missing_fields:
            data["_validation_errors"] = data.get("_validation_errors", [])
            data["_validation_errors"].append(f"Missing required fields: {missing_fields}")
        
        return data
    
    def _convert_units(self, data: Dict[str, Any], conversions: Dict[str, Any]) -> Dict[str, Any]:
        """Convert units in data."""
        for field, conversion in conversions.items():
            if field in data:
                try:
                    value = data[field]
                    if isinstance(value, (int, float)):
                        factor = conversion.get("factor", 1.0)
                        data[field] = value * factor
                        data[f"{field}_unit"] = conversion.get("target_unit", "unknown")
                except Exception as e:
                    self.logger.warning("Failed to convert units", 
                                      field=field, error=str(e))
        
        return data
    
    def _add_metadata(self, data: Dict[str, Any], metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Add metadata to data."""
        for key, value in metadata.items():
            data[f"_metadata_{key}"] = value
        
        return data
    
    def _basic_validation(self, data: Dict[str, Any], rules: Dict[str, Any]) -> Dict[str, Any]:
        """Perform basic validation on data."""
        errors = []
        
        for field, rule in rules.items():
            if field in data:
                value = data[field]
                
                if rule.get("required") and (value is None or value == ""):
                    errors.append(f"Required field {field} is missing or empty")
                
                if rule.get("type") == "numeric" and not isinstance(value, (int, float)):
                    errors.append(f"Field {field} must be numeric")
                
                if rule.get("min") is not None and value < rule["min"]:
                    errors.append(f"Field {field} is below minimum value {rule['min']}")
                
                if rule.get("max") is not None and value > rule["max"]:
                    errors.append(f"Field {field} is above maximum value {rule['max']}")
        
        if errors:
            data["_validation_errors"] = data.get("_validation_errors", [])
            data["_validation_errors"].extend(errors)
        
        return data
    
    def _schema_validation(self, data: Dict[str, Any], schema: Dict[str, Any]) -> Dict[str, Any]:
        """Perform schema validation on data."""
        # Simplified schema validation
        # In production, you'd use a proper schema validation library
        errors = []
        
        for field, field_schema in schema.get("properties", {}).items():
            if field_schema.get("required", False) and field not in data:
                errors.append(f"Required field {field} is missing")
        
        if errors:
            data["_validation_errors"] = data.get("_validation_errors", [])
            data["_validation_errors"].extend(errors)
        
        return data
    
    def _basic_enrichment(self, data: Dict[str, Any], enrichments: Dict[str, Any]) -> Dict[str, Any]:
        """Perform basic enrichment on data."""
        for key, value in enrichments.items():
            data[f"_enriched_{key}"] = value
        
        return data
    
    def _geospatial_enrichment(self, data: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
        """Perform geospatial enrichment on data."""
        # Simplified geospatial enrichment
        # In production, you'd use proper geospatial libraries
        if "delivery_location" in data:
            location = data["delivery_location"]
            data["_geospatial"] = {
                "location": location,
                "region": config.get("default_region", "unknown"),
                "coordinates": config.get("default_coordinates", [0, 0])
            }
        
        return data
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get rules engine statistics.
        
        Returns:
            Dict[str, Any]: Rules engine statistics
        """
        return {
            **self.stats,
            "total_rules": len(self.rules),
            "enabled_rules": len([r for r in self.rules if r.enabled]),
            "success_rate": (
                self.stats["successful_executions"] / max(1, self.stats["total_executions"])
            )
        }
    
    def reset_stats(self) -> None:
        """Reset rules engine statistics."""
        self.stats = {
            "total_executions": 0,
            "successful_executions": 0,
            "failed_executions": 0,
            "rule_executions": {}
        }
    
    async def get_health_status(self) -> Dict[str, Any]:
        """Get health status of the rules engine."""
        return {
            "status": "healthy",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "uptime_seconds": 0,
            "processing_stats": self.get_stats(),
            "rules_loaded": len(self.rules),
            "errors": []
        }
    
    async def is_ready(self) -> bool:
        """Check if the rules engine is ready."""
        return True
    
    async def get_metrics(self) -> Dict[str, Any]:
        """Get rules engine metrics."""
        stats = self.get_stats()
        stats["uptime_seconds"] = 0  # Would need to track start time
        return stats