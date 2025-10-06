"""
Taxonomy service for enrichment.

This module provides taxonomy classification capabilities for market data
including instrument types, location hierarchies, and price/quantity ranges.
"""

import asyncio
import json
import logging
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone

import structlog
import yaml
from pydantic import BaseModel, Field


class TaxonomyNode(BaseModel):
    """Individual taxonomy node."""
    
    name: str = Field(..., description="Node name")
    category: str = Field(..., description="Node category")
    parent: Optional[str] = Field(None, description="Parent node")
    children: List[str] = Field(default_factory=list, description="Child nodes")
    attributes: Dict[str, Any] = Field(default_factory=dict, description="Node attributes")
    tags: List[str] = Field(default_factory=list, description="Node tags")


class TaxonomyService:
    """
    Taxonomy service for market data classification.
    
    This service provides hierarchical classification capabilities for
    market data including instruments, locations, and value ranges.
    """
    
    def __init__(self, config_path: str = "configs/enrichment_taxonomy.yaml"):
        """
        Initialize the taxonomy service.
        
        Args:
            config_path: Path to taxonomy configuration
        """
        self.config_path = config_path
        self.logger = structlog.get_logger(__name__)
        
        # Load configuration
        self.config = self._load_config()
        
        # Taxonomy trees
        self.taxonomy_trees: Dict[str, Dict[str, TaxonomyNode]] = {}
        self._build_taxonomy_trees()
        
        # Service statistics
        self.stats = {
            "total_classifications": 0,
            "successful_classifications": 0,
            "failed_classifications": 0,
            "cache_hits": 0,
            "cache_misses": 0
        }
    
    def _load_config(self) -> Dict[str, Any]:
        """
        Load taxonomy configuration.
        
        Returns:
            Dict[str, Any]: Configuration data
        """
        try:
            with open(self.config_path, 'r') as f:
                config = yaml.safe_load(f)
            self.logger.info("Taxonomy configuration loaded", config_path=self.config_path)
            return config
        except Exception as e:
            self.logger.error("Failed to load taxonomy configuration", 
                            error=str(e), config_path=self.config_path)
            return {}
    
    def _build_taxonomy_trees(self) -> None:
        """Build taxonomy trees from configuration."""
        try:
            # Build instrument taxonomy
            self.taxonomy_trees["instruments"] = self._build_instrument_taxonomy()
            
            # Build location taxonomy
            self.taxonomy_trees["locations"] = self._build_location_taxonomy()
            
            # Build price range taxonomy
            self.taxonomy_trees["price_ranges"] = self._build_price_range_taxonomy()
            
            # Build quantity range taxonomy
            self.taxonomy_trees["quantity_ranges"] = self._build_quantity_range_taxonomy()
            
            # Build time period taxonomy
            self.taxonomy_trees["time_periods"] = self._build_time_period_taxonomy()
            
            self.logger.info("Taxonomy trees built successfully", 
                           tree_count=len(self.taxonomy_trees))
            
        except Exception as e:
            self.logger.error("Failed to build taxonomy trees", error=str(e))
            self.taxonomy_trees = {}
    
    def _build_instrument_taxonomy(self) -> Dict[str, TaxonomyNode]:
        """Build instrument taxonomy tree."""
        instruments = {}
        
        # Base instrument categories
        base_instruments = {
            "energy": TaxonomyNode(
                name="energy",
                category="instrument",
                attributes={"type": "commodity", "sector": "energy"},
                tags=["commodity", "energy", "trading"]
            ),
            "power": TaxonomyNode(
                name="power",
                category="instrument",
                parent="energy",
                attributes={"type": "electricity", "unit": "MWh"},
                tags=["electricity", "power", "trading"]
            ),
            "trade": TaxonomyNode(
                name="trade",
                category="instrument",
                parent="power",
                attributes={"type": "executed_trade", "settlement": "physical"},
                tags=["trade", "executed", "physical"]
            ),
            "curve": TaxonomyNode(
                name="curve",
                category="instrument",
                parent="power",
                attributes={"type": "price_curve", "settlement": "financial"},
                tags=["curve", "pricing", "financial"]
            ),
            "market_price": TaxonomyNode(
                name="market_price",
                category="instrument",
                parent="power",
                attributes={"type": "spot_price", "settlement": "real_time"},
                tags=["price", "spot", "real_time"]
            ),
            "system_status": TaxonomyNode(
                name="system_status",
                category="instrument",
                parent="power",
                attributes={"type": "operational_status", "settlement": "informational"},
                tags=["status", "operational", "informational"]
            )
        }
        
        # Add children relationships
        for node in base_instruments.values():
            if node.parent:
                parent_node = base_instruments.get(node.parent)
                if parent_node:
                    parent_node.children.append(node.name)
        
        return base_instruments
    
    def _build_location_taxonomy(self) -> Dict[str, TaxonomyNode]:
        """Build location taxonomy tree."""
        locations = {}
        
        # Base location categories
        base_locations = {
            "north_america": TaxonomyNode(
                name="north_america",
                category="location",
                attributes={"continent": "North America", "region": "continental"},
                tags=["continent", "north_america", "continental"]
            ),
            "united_states": TaxonomyNode(
                name="united_states",
                category="location",
                parent="north_america",
                attributes={"country": "United States", "region": "national"},
                tags=["country", "united_states", "national"]
            ),
            "midwest": TaxonomyNode(
                name="midwest",
                category="location",
                parent="united_states",
                attributes={"region": "Midwest", "market": "MISO"},
                tags=["region", "midwest", "miso"]
            ),
            "california": TaxonomyNode(
                name="california",
                category="location",
                parent="united_states",
                attributes={"region": "California", "market": "CAISO"},
                tags=["region", "california", "caiso"]
            ),
            "hub": TaxonomyNode(
                name="hub",
                category="location",
                attributes={"type": "trading_hub", "liquidity": "high"},
                tags=["hub", "trading", "high_liquidity"]
            ),
            "node": TaxonomyNode(
                name="node",
                category="location",
                attributes={"type": "transmission_node", "liquidity": "medium"},
                tags=["node", "transmission", "medium_liquidity"]
            )
        }
        
        # Add children relationships
        for node in base_locations.values():
            if node.parent:
                parent_node = base_locations.get(node.parent)
                if parent_node:
                    parent_node.children.append(node.name)
        
        return base_locations
    
    def _build_price_range_taxonomy(self) -> Dict[str, TaxonomyNode]:
        """Build price range taxonomy tree."""
        price_ranges = {}
        
        # Price range categories
        base_ranges = {
            "very_low": TaxonomyNode(
                name="very_low",
                category="price_range",
                attributes={"min": 0, "max": 20, "description": "Very low prices"},
                tags=["price", "very_low", "cheap"]
            ),
            "low": TaxonomyNode(
                name="low",
                category="price_range",
                attributes={"min": 20, "max": 40, "description": "Low prices"},
                tags=["price", "low", "affordable"]
            ),
            "medium": TaxonomyNode(
                name="medium",
                category="price_range",
                attributes={"min": 40, "max": 80, "description": "Medium prices"},
                tags=["price", "medium", "moderate"]
            ),
            "high": TaxonomyNode(
                name="high",
                category="price_range",
                attributes={"min": 80, "max": 150, "description": "High prices"},
                tags=["price", "high", "expensive"]
            ),
            "very_high": TaxonomyNode(
                name="very_high",
                category="price_range",
                attributes={"min": 150, "max": 1000, "description": "Very high prices"},
                tags=["price", "very_high", "premium"]
            )
        }
        
        return base_ranges
    
    def _build_quantity_range_taxonomy(self) -> Dict[str, TaxonomyNode]:
        """Build quantity range taxonomy tree."""
        quantity_ranges = {}
        
        # Quantity range categories
        base_ranges = {
            "very_small": TaxonomyNode(
                name="very_small",
                category="quantity_range",
                attributes={"min": 0, "max": 100, "description": "Very small quantities"},
                tags=["quantity", "very_small", "minimal"]
            ),
            "small": TaxonomyNode(
                name="small",
                category="quantity_range",
                attributes={"min": 100, "max": 500, "description": "Small quantities"},
                tags=["quantity", "small", "light"]
            ),
            "medium": TaxonomyNode(
                name="medium",
                category="quantity_range",
                attributes={"min": 500, "max": 1000, "description": "Medium quantities"},
                tags=["quantity", "medium", "moderate"]
            ),
            "large": TaxonomyNode(
                name="large",
                category="quantity_range",
                attributes={"min": 1000, "max": 5000, "description": "Large quantities"},
                tags=["quantity", "large", "substantial"]
            ),
            "very_large": TaxonomyNode(
                name="very_large",
                category="quantity_range",
                attributes={"min": 5000, "max": 10000, "description": "Very large quantities"},
                tags=["quantity", "very_large", "massive"]
            )
        }
        
        return base_ranges
    
    def _build_time_period_taxonomy(self) -> Dict[str, TaxonomyNode]:
        """Build time period taxonomy tree."""
        time_periods = {}
        
        # Time period categories
        base_periods = {
            "off_peak": TaxonomyNode(
                name="off_peak",
                category="time_period",
                attributes={"start": 0, "end": 6, "description": "Off-peak hours"},
                tags=["time", "off_peak", "low_demand"]
            ),
            "morning": TaxonomyNode(
                name="morning",
                category="time_period",
                attributes={"start": 6, "end": 12, "description": "Morning hours"},
                tags=["time", "morning", "rising_demand"]
            ),
            "afternoon": TaxonomyNode(
                name="afternoon",
                category="time_period",
                attributes={"start": 12, "end": 18, "description": "Afternoon hours"},
                tags=["time", "afternoon", "peak_demand"]
            ),
            "evening": TaxonomyNode(
                name="evening",
                category="time_period",
                attributes={"start": 18, "end": 24, "description": "Evening hours"},
                tags=["time", "evening", "declining_demand"]
            ),
            "peak": TaxonomyNode(
                name="peak",
                category="time_period",
                attributes={"start": 7, "end": 9, "description": "Peak hours"},
                tags=["time", "peak", "high_demand"]
            )
        }
        
        return base_periods
    
    def get_market_taxonomy(self, market: str) -> Dict[str, Any]:
        """
        Get market-specific taxonomy.
        
        Args:
            market: Market identifier
            
        Returns:
            Dict[str, Any]: Market-specific taxonomy
        """
        try:
            self.stats["total_classifications"] += 1
            
            # Get base taxonomy
            market_taxonomy = {
                "instruments": self.taxonomy_trees.get("instruments", {}),
                "locations": self.taxonomy_trees.get("locations", {}),
                "price_ranges": self.taxonomy_trees.get("price_ranges", {}),
                "quantity_ranges": self.taxonomy_trees.get("quantity_ranges", {}),
                "time_periods": self.taxonomy_trees.get("time_periods", {})
            }
            
            # Add market-specific customizations
            if market == "MISO":
                market_taxonomy["market_specific"] = {
                    "region": "Midwest",
                    "timezone": "America/Chicago",
                    "currency": "USD",
                    "unit": "MWh",
                    "price_unit": "$/MWh"
                }
            elif market == "CAISO":
                market_taxonomy["market_specific"] = {
                    "region": "California",
                    "timezone": "America/Los_Angeles",
                    "currency": "USD",
                    "unit": "MWh",
                    "price_unit": "$/MWh"
                }
            
            self.stats["successful_classifications"] += 1
            return market_taxonomy
            
        except Exception as e:
            self.stats["failed_classifications"] += 1
            self.logger.error("Failed to get market taxonomy", market=market, error=str(e))
            return {}
    
    def classify_instrument(self, data_type: str, market: str) -> List[str]:
        """
        Classify instrument type.
        
        Args:
            data_type: Type of data
            market: Market identifier
            
        Returns:
            List[str]: Classification tags
        """
        try:
            self.stats["total_classifications"] += 1
            
            market_taxonomy = self.get_market_taxonomy(market)
            instruments = market_taxonomy.get("instruments", {})
            
            # Find matching instrument
            if data_type in instruments:
                instrument_node = instruments[data_type]
                tags = instrument_node.tags.copy()
                
                # Add parent tags
                if instrument_node.parent:
                    parent_node = instruments.get(instrument_node.parent)
                    if parent_node:
                        tags.extend(parent_node.tags)
                
                self.stats["successful_classifications"] += 1
                return list(set(tags))
            
            self.stats["successful_classifications"] += 1
            return ["unknown_instrument"]
            
        except Exception as e:
            self.stats["failed_classifications"] += 1
            self.logger.error("Failed to classify instrument", 
                            data_type=data_type, 
                            market=market, 
                            error=str(e))
            return ["classification_error"]
    
    def classify_location(self, location: str, market: str) -> List[str]:
        """
        Classify location.
        
        Args:
            location: Location identifier
            market: Market identifier
            
        Returns:
            List[str]: Classification tags
        """
        try:
            self.stats["total_classifications"] += 1
            
            market_taxonomy = self.get_market_taxonomy(market)
            locations = market_taxonomy.get("locations", {})
            
            # Find matching location
            location_lower = location.lower()
            for location_key, location_node in locations.items():
                if location_key in location_lower:
                    tags = location_node.tags.copy()
                    
                    # Add parent tags
                    if location_node.parent:
                        parent_node = locations.get(location_node.parent)
                        if parent_node:
                            tags.extend(parent_node.tags)
                    
                    self.stats["successful_classifications"] += 1
                    return list(set(tags))
            
            self.stats["successful_classifications"] += 1
            return ["unknown_location"]
            
        except Exception as e:
            self.stats["failed_classifications"] += 1
            self.logger.error("Failed to classify location", 
                            location=location, 
                            market=market, 
                            error=str(e))
            return ["classification_error"]
    
    def classify_price_range(self, price: float, market: str) -> List[str]:
        """
        Classify price range.
        
        Args:
            price: Price value
            market: Market identifier
            
        Returns:
            List[str]: Classification tags
        """
        try:
            self.stats["total_classifications"] += 1
            
            market_taxonomy = self.get_market_taxonomy(market)
            price_ranges = market_taxonomy.get("price_ranges", {})
            
            # Find matching price range
            for range_key, range_node in price_ranges.items():
                min_price = range_node.attributes.get("min", 0)
                max_price = range_node.attributes.get("max", float('inf'))
                
                if min_price <= price <= max_price:
                    self.stats["successful_classifications"] += 1
                    return range_node.tags
            
            self.stats["successful_classifications"] += 1
            return ["unknown_price_range"]
            
        except Exception as e:
            self.stats["failed_classifications"] += 1
            self.logger.error("Failed to classify price range", 
                            price=price, 
                            market=market, 
                            error=str(e))
            return ["classification_error"]
    
    def classify_quantity_range(self, quantity: float, market: str) -> List[str]:
        """
        Classify quantity range.
        
        Args:
            quantity: Quantity value
            market: Market identifier
            
        Returns:
            List[str]: Classification tags
        """
        try:
            self.stats["total_classifications"] += 1
            
            market_taxonomy = self.get_market_taxonomy(market)
            quantity_ranges = market_taxonomy.get("quantity_ranges", {})
            
            # Find matching quantity range
            for range_key, range_node in quantity_ranges.items():
                min_quantity = range_node.attributes.get("min", 0)
                max_quantity = range_node.attributes.get("max", float('inf'))
                
                if min_quantity <= quantity <= max_quantity:
                    self.stats["successful_classifications"] += 1
                    return range_node.tags
            
            self.stats["successful_classifications"] += 1
            return ["unknown_quantity_range"]
            
        except Exception as e:
            self.stats["failed_classifications"] += 1
            self.logger.error("Failed to classify quantity range", 
                            quantity=quantity, 
                            market=market, 
                            error=str(e))
            return ["classification_error"]
    
    def classify_time_period(self, hour: int, market: str) -> List[str]:
        """
        Classify time period.
        
        Args:
            hour: Hour of day (0-23)
            market: Market identifier
            
        Returns:
            List[str]: Classification tags
        """
        try:
            self.stats["total_classifications"] += 1
            
            market_taxonomy = self.get_market_taxonomy(market)
            time_periods = market_taxonomy.get("time_periods", {})
            
            # Find matching time period
            for period_key, period_node in time_periods.items():
                start_hour = period_node.attributes.get("start", 0)
                end_hour = period_node.attributes.get("end", 23)
                
                if start_hour <= hour <= end_hour:
                    self.stats["successful_classifications"] += 1
                    return period_node.tags
            
            self.stats["successful_classifications"] += 1
            return ["unknown_time_period"]
            
        except Exception as e:
            self.stats["failed_classifications"] += 1
            self.logger.error("Failed to classify time period", 
                            hour=hour, 
                            market=market, 
                            error=str(e))
            return ["classification_error"]
    
    def get_taxonomy_hierarchy(self, category: str) -> Dict[str, Any]:
        """
        Get taxonomy hierarchy for a category.
        
        Args:
            category: Taxonomy category
            
        Returns:
            Dict[str, Any]: Taxonomy hierarchy
        """
        try:
            if category in self.taxonomy_trees:
                tree = self.taxonomy_trees[category]
                
                # Build hierarchy structure
                hierarchy = {}
                for node_name, node in tree.items():
                    hierarchy[node_name] = {
                        "name": node.name,
                        "category": node.category,
                        "parent": node.parent,
                        "children": node.children,
                        "attributes": node.attributes,
                        "tags": node.tags
                    }
                
                return hierarchy
            
            return {}
            
        except Exception as e:
            self.logger.error("Failed to get taxonomy hierarchy", 
                            category=category, 
                            error=str(e))
            return {}
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get taxonomy service statistics.
        
        Returns:
            Dict[str, Any]: Service statistics
        """
        return {
            **self.stats,
            "taxonomy_trees_count": len(self.taxonomy_trees),
            "success_rate": (
                self.stats["successful_classifications"] / max(1, self.stats["total_classifications"])
            )
        }
    
    def reset_stats(self) -> None:
        """Reset taxonomy service statistics."""
        self.stats = {
            "total_classifications": 0,
            "successful_classifications": 0,
            "failed_classifications": 0,
            "cache_hits": 0,
            "cache_misses": 0
        }