"""
Core enrichment service implementation.

This module provides the main enrichment logic for adding semantic tags,
taxonomy classifications, and metadata to normalized market data.
"""

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import uuid4

import structlog
import yaml
from pydantic import BaseModel, Field

from .taxonomy import TaxonomyService


class EnrichmentResult(BaseModel):
    """Result of enrichment operation."""
    
    enriched_data: Dict[str, Any] = Field(..., description="Enriched data")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Enrichment metadata")
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    taxonomy_tags: List[str] = Field(default_factory=list, description="Taxonomy tags applied")
    semantic_tags: List[str] = Field(default_factory=list, description="Semantic tags applied")
    geospatial_data: Optional[Dict[str, Any]] = Field(None, description="Geospatial enrichment data")
    enrichment_score: float = Field(..., description="Enrichment quality score")


class EnrichmentService:
    """
    Core enrichment service for market data.
    
    This service handles the enrichment of normalized market data with
    semantic tags, taxonomy classifications, and metadata.
    """
    
    def __init__(self, config_path: str = "configs/enrichment_taxonomy.yaml"):
        """
        Initialize the enrichment service.
        
        Args:
            config_path: Path to enrichment taxonomy configuration
        """
        self.config_path = config_path
        self.logger = structlog.get_logger(__name__)
        
        # Initialize components
        self.taxonomy_service = TaxonomyService(config_path)
        
        # Load configuration
        self.config = self._load_config()
        
        # Enrichment statistics
        self.stats = {
            "total_enrichments": 0,
            "successful_enrichments": 0,
            "failed_enrichments": 0,
            "taxonomy_applications": 0,
            "semantic_tag_applications": 0,
            "geospatial_enrichments": 0,
            "enrichment_scores": []
        }
    
    def _load_config(self) -> Dict[str, Any]:
        """
        Load enrichment configuration.
        
        Returns:
            Dict[str, Any]: Configuration data
        """
        try:
            with open(self.config_path, 'r') as f:
                config = yaml.safe_load(f)
            self.logger.info("Enrichment configuration loaded", config_path=self.config_path)
            return config
        except Exception as e:
            self.logger.error("Failed to load enrichment configuration", 
                            error=str(e), config_path=self.config_path)
            return {}
    
    async def enrich(self, normalized_data: Dict[str, Any]) -> EnrichmentResult:
        """
        Enrich normalized market data.
        
        Args:
            normalized_data: Normalized data to enrich
            
        Returns:
            EnrichmentResult: Enrichment result
        """
        try:
            self.stats["total_enrichments"] += 1
            
            # Start with normalized data
            enriched_data = normalized_data.copy()
            
            # Apply taxonomy enrichment
            taxonomy_tags = await self._apply_taxonomy_enrichment(enriched_data)
            enriched_data["taxonomy_tags"] = taxonomy_tags
            self.stats["taxonomy_applications"] += len(taxonomy_tags)
            
            # Apply semantic tagging
            semantic_tags = await self._apply_semantic_tagging(enriched_data)
            enriched_data["semantic_tags"] = semantic_tags
            self.stats["semantic_tag_applications"] += len(semantic_tags)
            
            # Apply geospatial enrichment
            geospatial_data = await self._apply_geospatial_enrichment(enriched_data)
            if geospatial_data:
                enriched_data["geospatial_data"] = geospatial_data
                self.stats["geospatial_enrichments"] += 1
            
            # Apply metadata enrichment
            metadata_tags = await self._apply_metadata_enrichment(enriched_data)
            enriched_data["metadata_tags"] = metadata_tags
            
            # Calculate enrichment score
            enrichment_score = self._calculate_enrichment_score(enriched_data)
            enriched_data["enrichment_score"] = enrichment_score
            
            # Add enrichment timestamp
            enriched_data["enrichment_timestamp"] = datetime.now(timezone.utc).isoformat()
            
            # Create result
            result = EnrichmentResult(
                enriched_data=enriched_data,
                metadata={
                    "enrichment_method": "comprehensive",
                    "taxonomy_tags_count": len(taxonomy_tags),
                    "semantic_tags_count": len(semantic_tags),
                    "geospatial_enriched": geospatial_data is not None,
                    "metadata_tags_count": len(metadata_tags),
                    "enrichment_timestamp": datetime.now(timezone.utc).isoformat()
                },
                taxonomy_tags=taxonomy_tags,
                semantic_tags=semantic_tags,
                geospatial_data=geospatial_data,
                enrichment_score=enrichment_score
            )
            
            # Update statistics
            self.stats["successful_enrichments"] += 1
            self.stats["enrichment_scores"].append(enrichment_score)
            
            self.logger.info("Data enriched successfully", 
                           taxonomy_tags=len(taxonomy_tags),
                           semantic_tags=len(semantic_tags),
                           enrichment_score=enrichment_score)
            
            return result
            
        except Exception as e:
            self.stats["failed_enrichments"] += 1
            self.logger.error("Failed to enrich data", error=str(e), normalized_data=normalized_data)
            
            # Return error result
            return EnrichmentResult(
                enriched_data=normalized_data,
                metadata={"error": str(e)},
                taxonomy_tags=[],
                semantic_tags=[],
                enrichment_score=0.0
            )
    
    async def _apply_taxonomy_enrichment(self, data: Dict[str, Any]) -> List[str]:
        """
        Apply taxonomy enrichment to data.
        
        Args:
            data: Data to enrich
            
        Returns:
            List[str]: Applied taxonomy tags
        """
        try:
            taxonomy_tags = []
            
            # Get market-specific taxonomy
            market = data.get("market", "UNKNOWN")
            market_taxonomy = self.taxonomy_service.get_market_taxonomy(market)
            
            # Apply instrument taxonomy
            if "data_type" in data:
                data_type = data["data_type"]
                instrument_tags = market_taxonomy.get("instruments", {}).get(data_type, [])
                taxonomy_tags.extend(instrument_tags)
            
            # Apply location taxonomy
            if "delivery_location" in data:
                location = data["delivery_location"]
                location_tags = market_taxonomy.get("locations", {}).get(location, [])
                taxonomy_tags.extend(location_tags)
            
            # Apply price taxonomy
            if "price" in data and data["price"] is not None:
                price = float(data["price"])
                price_tags = self._get_price_taxonomy_tags(price, market_taxonomy)
                taxonomy_tags.extend(price_tags)
            
            # Apply quantity taxonomy
            if "quantity" in data and data["quantity"] is not None:
                quantity = float(data["quantity"])
                quantity_tags = self._get_quantity_taxonomy_tags(quantity, market_taxonomy)
                taxonomy_tags.extend(quantity_tags)
            
            # Apply time-based taxonomy
            if "delivery_hour" in data and data["delivery_hour"] is not None:
                hour = int(data["delivery_hour"])
                time_tags = self._get_time_taxonomy_tags(hour, market_taxonomy)
                taxonomy_tags.extend(time_tags)
            
            # Remove duplicates and return
            return list(set(taxonomy_tags))
            
        except Exception as e:
            self.logger.error("Failed to apply taxonomy enrichment", error=str(e))
            return []
    
    async def _apply_semantic_tagging(self, data: Dict[str, Any]) -> List[str]:
        """
        Apply semantic tagging to data.
        
        Args:
            data: Data to tag
            
        Returns:
            List[str]: Applied semantic tags
        """
        try:
            semantic_tags = []
            
            # Market-specific semantic tags
            market = data.get("market", "UNKNOWN")
            if market == "MISO":
                semantic_tags.extend(["midwest_energy", "wind_heavy", "coal_legacy"])
            elif market == "CAISO":
                semantic_tags.extend(["california_energy", "solar_heavy", "renewable_leader"])
            
            # Data type semantic tags
            data_type = data.get("data_type", "unknown")
            if data_type == "trade":
                semantic_tags.extend(["trading_activity", "market_liquidity"])
            elif data_type == "curve":
                semantic_tags.extend(["price_curve", "forward_pricing"])
            elif data_type == "market_price":
                semantic_tags.extend(["spot_pricing", "real_time_market"])
            elif data_type == "system_status":
                semantic_tags.extend(["system_health", "operational_status"])
            
            # Price-based semantic tags
            if "price" in data and data["price"] is not None:
                price = float(data["price"])
                if price > 100:
                    semantic_tags.append("high_price")
                elif price < 20:
                    semantic_tags.append("low_price")
                else:
                    semantic_tags.append("normal_price")
            
            # Quantity-based semantic tags
            if "quantity" in data and data["quantity"] is not None:
                quantity = float(data["quantity"])
                if quantity > 1000:
                    semantic_tags.append("large_volume")
                elif quantity < 100:
                    semantic_tags.append("small_volume")
                else:
                    semantic_tags.append("medium_volume")
            
            # Time-based semantic tags
            if "delivery_hour" in data and data["delivery_hour"] is not None:
                hour = int(data["delivery_hour"])
                if 6 <= hour <= 18:
                    semantic_tags.append("daytime")
                else:
                    semantic_tags.append("nighttime")
                
                if 7 <= hour <= 9 or 17 <= hour <= 19:
                    semantic_tags.append("peak_hours")
                else:
                    semantic_tags.append("off_peak_hours")
            
            # Location-based semantic tags
            if "delivery_location" in data:
                location = data["delivery_location"]
                if "hub" in location.lower():
                    semantic_tags.append("hub_location")
                elif "node" in location.lower():
                    semantic_tags.append("node_location")
                else:
                    semantic_tags.append("generic_location")
            
            # Remove duplicates and return
            return list(set(semantic_tags))
            
        except Exception as e:
            self.logger.error("Failed to apply semantic tagging", error=str(e))
            return []
    
    async def _apply_geospatial_enrichment(self, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Apply geospatial enrichment to data.
        
        Args:
            data: Data to enrich
            
        Returns:
            Optional[Dict[str, Any]]: Geospatial enrichment data
        """
        try:
            geospatial_data = {}
            
            # Location-based geospatial data
            if "delivery_location" in data:
                location = data["delivery_location"]
                geospatial_data["location"] = location
                
                # Get coordinates for location (simplified - in production, use proper geocoding)
                coordinates = self._get_location_coordinates(location)
                if coordinates:
                    geospatial_data["coordinates"] = coordinates
                
                # Get region information
                region = self._get_location_region(location)
                if region:
                    geospatial_data["region"] = region
                
                # Get timezone information
                timezone = self._get_location_timezone(location)
                if timezone:
                    geospatial_data["timezone"] = timezone
            
            # Market-based geospatial data
            market = data.get("market", "UNKNOWN")
            if market == "MISO":
                geospatial_data["market_region"] = "Midwest"
                geospatial_data["market_coordinates"] = [39.8283, -98.5795]  # Center of US
            elif market == "CAISO":
                geospatial_data["market_region"] = "California"
                geospatial_data["market_coordinates"] = [36.7783, -119.4179]  # Center of California
            
            return geospatial_data if geospatial_data else None
            
        except Exception as e:
            self.logger.error("Failed to apply geospatial enrichment", error=str(e))
            return None
    
    async def _apply_metadata_enrichment(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Apply metadata enrichment to data.
        
        Args:
            data: Data to enrich
            
        Returns:
            Dict[str, Any]: Metadata tags
        """
        try:
            metadata_tags = {}
            
            # Data quality metadata
            metadata_tags["data_quality"] = {
                "completeness": self._calculate_completeness_score(data),
                "accuracy": self._calculate_accuracy_score(data),
                "consistency": self._calculate_consistency_score(data)
            }
            
            # Business metadata
            metadata_tags["business_context"] = {
                "market_segment": self._get_market_segment(data),
                "trading_session": self._get_trading_session(data),
                "price_tier": self._get_price_tier(data)
            }
            
            # Technical metadata
            metadata_tags["technical_context"] = {
                "data_source": data.get("source", "unknown"),
                "processing_stage": "enriched",
                "enrichment_version": "1.0.0"
            }
            
            # Temporal metadata
            if "delivery_date" in data:
                delivery_date = data["delivery_date"]
                metadata_tags["temporal_context"] = {
                    "delivery_date": delivery_date,
                    "delivery_day_of_week": self._get_day_of_week(delivery_date),
                    "delivery_month": self._get_month(delivery_date),
                    "delivery_quarter": self._get_quarter(delivery_date)
                }
            
            return metadata_tags
            
        except Exception as e:
            self.logger.error("Failed to apply metadata enrichment", error=str(e))
            return {}
    
    def _get_price_taxonomy_tags(self, price: float, market_taxonomy: Dict[str, Any]) -> List[str]:
        """Get price-based taxonomy tags."""
        price_ranges = market_taxonomy.get("price_ranges", {})
        
        for range_name, range_config in price_ranges.items():
            min_price = range_config.get("min", 0)
            max_price = range_config.get("max", float('inf'))
            
            if min_price <= price <= max_price:
                return [range_name]
        
        return ["unknown_price_range"]
    
    def _get_quantity_taxonomy_tags(self, quantity: float, market_taxonomy: Dict[str, Any]) -> List[str]:
        """Get quantity-based taxonomy tags."""
        quantity_ranges = market_taxonomy.get("quantity_ranges", {})
        
        for range_name, range_config in quantity_ranges.items():
            min_quantity = range_config.get("min", 0)
            max_quantity = range_config.get("max", float('inf'))
            
            if min_quantity <= quantity <= max_quantity:
                return [range_name]
        
        return ["unknown_quantity_range"]
    
    def _get_time_taxonomy_tags(self, hour: int, market_taxonomy: Dict[str, Any]) -> List[str]:
        """Get time-based taxonomy tags."""
        time_periods = market_taxonomy.get("time_periods", {})
        
        for period_name, period_config in time_periods.items():
            start_hour = period_config.get("start", 0)
            end_hour = period_config.get("end", 23)
            
            if start_hour <= hour <= end_hour:
                return [period_name]
        
        return ["unknown_time_period"]
    
    def _get_location_coordinates(self, location: str) -> Optional[List[float]]:
        """Get coordinates for a location (simplified implementation)."""
        # Simplified location mapping - in production, use proper geocoding service
        location_coords = {
            "hub": [39.8283, -98.5795],
            "node": [40.7128, -74.0060],
            "default": [39.8283, -98.5795]
        }
        
        for key, coords in location_coords.items():
            if key in location.lower():
                return coords
        
        return location_coords["default"]
    
    def _get_location_region(self, location: str) -> Optional[str]:
        """Get region for a location."""
        # Simplified region mapping
        if "hub" in location.lower():
            return "hub_region"
        elif "node" in location.lower():
            return "node_region"
        else:
            return "unknown_region"
    
    def _get_location_timezone(self, location: str) -> Optional[str]:
        """Get timezone for a location."""
        # Simplified timezone mapping
        if "hub" in location.lower():
            return "America/Chicago"
        elif "node" in location.lower():
            return "America/New_York"
        else:
            return "UTC"
    
    def _calculate_completeness_score(self, data: Dict[str, Any]) -> float:
        """Calculate data completeness score."""
        required_fields = ["event_id", "occurred_at", "tenant_id", "market", "data_type"]
        present_fields = sum(1 for field in required_fields if field in data and data[field] is not None)
        return present_fields / len(required_fields)
    
    def _calculate_accuracy_score(self, data: Dict[str, Any]) -> float:
        """Calculate data accuracy score."""
        # Simplified accuracy calculation
        score = 1.0
        
        # Check for reasonable price values
        if "price" in data and data["price"] is not None:
            price = float(data["price"])
            if price < 0 or price > 1000:
                score -= 0.2
        
        # Check for reasonable quantity values
        if "quantity" in data and data["quantity"] is not None:
            quantity = float(data["quantity"])
            if quantity < 0 or quantity > 10000:
                score -= 0.2
        
        return max(0.0, score)
    
    def _calculate_consistency_score(self, data: Dict[str, Any]) -> float:
        """Calculate data consistency score."""
        # Simplified consistency calculation
        score = 1.0
        
        # Check for consistent data types
        if "price" in data and "quantity" in data:
            if data["price"] is not None and data["quantity"] is not None:
                # Both should be numeric
                try:
                    float(data["price"])
                    float(data["quantity"])
                except (ValueError, TypeError):
                    score -= 0.3
        
        return max(0.0, score)
    
    def _get_market_segment(self, data: Dict[str, Any]) -> str:
        """Get market segment for data."""
        market = data.get("market", "UNKNOWN")
        data_type = data.get("data_type", "unknown")
        
        if market == "MISO":
            return "midwest_energy_market"
        elif market == "CAISO":
            return "california_energy_market"
        else:
            return "unknown_market"
    
    def _get_trading_session(self, data: Dict[str, Any]) -> str:
        """Get trading session for data."""
        if "delivery_hour" in data and data["delivery_hour"] is not None:
            hour = int(data["delivery_hour"])
            if 7 <= hour <= 9 or 17 <= hour <= 19:
                return "peak_session"
            else:
                return "off_peak_session"
        
        return "unknown_session"
    
    def _get_price_tier(self, data: Dict[str, Any]) -> str:
        """Get price tier for data."""
        if "price" in data and data["price"] is not None:
            price = float(data["price"])
            if price > 100:
                return "high_tier"
            elif price < 20:
                return "low_tier"
            else:
                return "mid_tier"
        
        return "unknown_tier"
    
    def _get_day_of_week(self, date_str: str) -> Optional[str]:
        """Get day of week for date."""
        try:
            from datetime import datetime
            dt = datetime.strptime(date_str, "%Y-%m-%d")
            return dt.strftime("%A")
        except Exception:
            return None
    
    def _get_month(self, date_str: str) -> Optional[str]:
        """Get month for date."""
        try:
            from datetime import datetime
            dt = datetime.strptime(date_str, "%Y-%m-%d")
            return dt.strftime("%B")
        except Exception:
            return None
    
    def _get_quarter(self, date_str: str) -> Optional[str]:
        """Get quarter for date."""
        try:
            from datetime import datetime
            dt = datetime.strptime(date_str, "%Y-%m-%d")
            quarter = (dt.month - 1) // 3 + 1
            return f"Q{quarter}"
        except Exception:
            return None
    
    def _calculate_enrichment_score(self, data: Dict[str, Any]) -> float:
        """
        Calculate enrichment quality score.
        
        Args:
            data: Enriched data
            
        Returns:
            float: Enrichment score between 0.0 and 1.0
        """
        score = 1.0
        
        # Deduct for missing taxonomy tags
        taxonomy_tags = data.get("taxonomy_tags", [])
        if not taxonomy_tags:
            score -= 0.2
        
        # Deduct for missing semantic tags
        semantic_tags = data.get("semantic_tags", [])
        if not semantic_tags:
            score -= 0.2
        
        # Deduct for missing geospatial data
        geospatial_data = data.get("geospatial_data")
        if not geospatial_data:
            score -= 0.1
        
        # Deduct for missing metadata
        metadata_tags = data.get("metadata_tags", {})
        if not metadata_tags:
            score -= 0.1
        
        # Bonus for comprehensive enrichment
        if len(taxonomy_tags) >= 3 and len(semantic_tags) >= 3:
            score += 0.1
        
        return max(0.0, min(1.0, score))
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get enrichment statistics.
        
        Returns:
            Dict[str, Any]: Enrichment statistics
        """
        if self.stats["enrichment_scores"]:
            avg_score = sum(self.stats["enrichment_scores"]) / len(self.stats["enrichment_scores"])
        else:
            avg_score = 0.0
        
        return {
            **self.stats,
            "average_enrichment_score": avg_score,
            "success_rate": (
                self.stats["successful_enrichments"] / max(1, self.stats["total_enrichments"])
            )
        }
    
    def reset_stats(self) -> None:
        """Reset enrichment statistics."""
        self.stats = {
            "total_enrichments": 0,
            "successful_enrichments": 0,
            "failed_enrichments": 0,
            "taxonomy_applications": 0,
            "semantic_tag_applications": 0,
            "geospatial_enrichments": 0,
            "enrichment_scores": []
        }