"""
Unit tests for enrichment service.
"""

import pytest
import asyncio
from unittest.mock import Mock, AsyncMock, patch
from datetime import datetime, timezone

from services.service-enrichment.src.core.enricher import EnrichmentService, EnrichmentResult
from services.service-enrichment.src.core.taxonomy import TaxonomyService


class TestEnrichmentService:
    """Test enrichment service."""

    @pytest.fixture
    def enrichment_service(self):
        """Create enrichment service."""
        with patch('services.service-enrichment.src.core.enricher.TaxonomyService') as mock_taxonomy:
            mock_taxonomy.return_value.get_market_taxonomy.return_value = {
                "instruments": {
                    "trade": ["energy_trading", "physical_settlement"],
                    "market_price": ["spot_pricing", "real_time_market"]
                },
                "locations": {
                    "hub": ["trading_hub", "high_liquidity"],
                    "node": ["transmission_node", "medium_liquidity"]
                },
                "price_ranges": {
                    "low": {"min": 0, "max": 50, "tags": ["low_price"]},
                    "medium": {"min": 50, "max": 100, "tags": ["medium_price"]},
                    "high": {"min": 100, "max": 1000, "tags": ["high_price"]}
                },
                "quantity_ranges": {
                    "small": {"min": 0, "max": 500, "tags": ["small_quantity"]},
                    "medium": {"min": 500, "max": 1000, "tags": ["medium_quantity"]},
                    "large": {"min": 1000, "max": 10000, "tags": ["large_quantity"]}
                },
                "time_periods": {
                    "off_peak": {"start": 0, "end": 6, "tags": ["off_peak"]},
                    "morning": {"start": 6, "end": 12, "tags": ["morning"]},
                    "afternoon": {"start": 12, "end": 18, "tags": ["afternoon"]},
                    "evening": {"start": 18, "end": 24, "tags": ["evening"]}
                }
            }
            return EnrichmentService()

    @pytest.fixture
    def sample_normalized_data(self):
        """Sample normalized data."""
        return {
            "event_id": "test-event-123",
            "occurred_at": int(datetime.now(timezone.utc).timestamp() * 1_000_000),
            "tenant_id": "default",
            "schema_version": "1.0.0",
            "producer": "normalization-service",
            "market": "CAISO",
            "data_type": "market_price",
            "delivery_location": "hub",
            "delivery_date": "2025-01-15",
            "delivery_hour": 14,
            "price": 75.50,
            "quantity": 250.0,
            "source": "caiso-oasis"
        }

    @pytest.mark.asyncio
    async def test_enrich_success(self, enrichment_service, sample_normalized_data):
        """Test successful enrichment."""
        result = await enrichment_service.enrich(sample_normalized_data)
        
        assert isinstance(result, EnrichmentResult)
        assert result.enriched_data["event_id"] == sample_normalized_data["event_id"]
        assert "taxonomy_tags" in result.enriched_data
        assert "semantic_tags" in result.enriched_data
        assert "geospatial_data" in result.enriched_data
        assert "metadata_tags" in result.enriched_data
        assert "enrichment_score" in result.enriched_data
        assert "enrichment_timestamp" in result.enriched_data
        assert result.enrichment_score > 0.0

    @pytest.mark.asyncio
    async def test_enrich_with_missing_fields(self, enrichment_service):
        """Test enrichment with missing fields."""
        minimal_data = {
            "event_id": "test-event-456",
            "occurred_at": int(datetime.now(timezone.utc).timestamp() * 1_000_000),
            "tenant_id": "default",
            "market": "MISO"
        }
        
        result = await enrichment_service.enrich(minimal_data)
        
        assert isinstance(result, EnrichmentResult)
        assert result.enriched_data["event_id"] == minimal_data["event_id"]
        assert result.enrichment_score >= 0.0

    @pytest.mark.asyncio
    async def test_taxonomy_enrichment(self, enrichment_service, sample_normalized_data):
        """Test taxonomy enrichment."""
        taxonomy_tags = await enrichment_service._apply_taxonomy_enrichment(sample_normalized_data)
        
        assert isinstance(taxonomy_tags, list)
        # Should include instrument tags
        assert any("energy" in tag for tag in taxonomy_tags)
        # Should include location tags
        assert any("hub" in tag for tag in taxonomy_tags)
        # Should include price range tags
        assert any("medium" in tag for tag in taxonomy_tags)
        # Should include quantity range tags
        assert any("small" in tag for tag in taxonomy_tags)
        # Should include time period tags
        assert any("afternoon" in tag for tag in taxonomy_tags)

    @pytest.mark.asyncio
    async def test_semantic_tagging(self, enrichment_service, sample_normalized_data):
        """Test semantic tagging."""
        semantic_tags = await enrichment_service._apply_semantic_tagging(sample_normalized_data)
        
        assert isinstance(semantic_tags, list)
        # Should include market-specific tags
        assert any("california" in tag for tag in semantic_tags)
        # Should include data type tags
        assert any("spot" in tag for tag in semantic_tags)
        # Should include price-based tags
        assert any("normal" in tag for tag in semantic_tags)
        # Should include quantity-based tags
        assert any("small" in tag for tag in semantic_tags)
        # Should include time-based tags
        assert any("daytime" in tag for tag in semantic_tags)
        # Should include location-based tags
        assert any("hub" in tag for tag in semantic_tags)

    @pytest.mark.asyncio
    async def test_geospatial_enrichment(self, enrichment_service, sample_normalized_data):
        """Test geospatial enrichment."""
        geospatial_data = await enrichment_service._apply_geospatial_enrichment(sample_normalized_data)
        
        assert isinstance(geospatial_data, dict)
        assert "location" in geospatial_data
        assert "coordinates" in geospatial_data
        assert "region" in geospatial_data
        assert "timezone" in geospatial_data
        assert "market_region" in geospatial_data
        assert "market_coordinates" in geospatial_data

    @pytest.mark.asyncio
    async def test_metadata_enrichment(self, enrichment_service, sample_normalized_data):
        """Test metadata enrichment."""
        metadata_tags = await enrichment_service._apply_metadata_enrichment(sample_normalized_data)
        
        assert isinstance(metadata_tags, dict)
        assert "data_quality" in metadata_tags
        assert "business_context" in metadata_tags
        assert "technical_context" in metadata_tags
        assert "temporal_context" in metadata_tags
        
        # Check data quality metrics
        assert "completeness" in metadata_tags["data_quality"]
        assert "accuracy" in metadata_tags["data_quality"]
        assert "consistency" in metadata_tags["data_quality"]
        
        # Check business context
        assert "market_segment" in metadata_tags["business_context"]
        assert "trading_session" in metadata_tags["business_context"]
        assert "price_tier" in metadata_tags["business_context"]
        
        # Check technical context
        assert "data_source" in metadata_tags["technical_context"]
        assert "processing_stage" in metadata_tags["technical_context"]
        assert "enrichment_version" in metadata_tags["technical_context"]

    def test_price_taxonomy_tags(self, enrichment_service):
        """Test price taxonomy tag generation."""
        market_taxonomy = {
            "price_ranges": {
                "low": {"min": 0, "max": 50, "tags": ["low_price"]},
                "medium": {"min": 50, "max": 100, "tags": ["medium_price"]},
                "high": {"min": 100, "max": 1000, "tags": ["high_price"]}
            }
        }
        
        # Test low price
        tags = enrichment_service._get_price_taxonomy_tags(25.0, market_taxonomy)
        assert tags == ["low"]
        
        # Test medium price
        tags = enrichment_service._get_price_taxonomy_tags(75.0, market_taxonomy)
        assert tags == ["medium"]
        
        # Test high price
        tags = enrichment_service._get_price_taxonomy_tags(150.0, market_taxonomy)
        assert tags == ["high"]

    def test_quantity_taxonomy_tags(self, enrichment_service):
        """Test quantity taxonomy tag generation."""
        market_taxonomy = {
            "quantity_ranges": {
                "small": {"min": 0, "max": 500, "tags": ["small_quantity"]},
                "medium": {"min": 500, "max": 1000, "tags": ["medium_quantity"]},
                "large": {"min": 1000, "max": 10000, "tags": ["large_quantity"]}
            }
        }
        
        # Test small quantity
        tags = enrichment_service._get_quantity_taxonomy_tags(250.0, market_taxonomy)
        assert tags == ["small"]
        
        # Test medium quantity
        tags = enrichment_service._get_quantity_taxonomy_tags(750.0, market_taxonomy)
        assert tags == ["medium"]
        
        # Test large quantity
        tags = enrichment_service._get_quantity_taxonomy_tags(1500.0, market_taxonomy)
        assert tags == ["large"]

    def test_time_taxonomy_tags(self, enrichment_service):
        """Test time taxonomy tag generation."""
        market_taxonomy = {
            "time_periods": {
                "off_peak": {"start": 0, "end": 6, "tags": ["off_peak"]},
                "morning": {"start": 6, "end": 12, "tags": ["morning"]},
                "afternoon": {"start": 12, "end": 18, "tags": ["afternoon"]},
                "evening": {"start": 18, "end": 24, "tags": ["evening"]}
            }
        }
        
        # Test off-peak hour
        tags = enrichment_service._get_time_taxonomy_tags(3, market_taxonomy)
        assert tags == ["off_peak"]
        
        # Test morning hour
        tags = enrichment_service._get_time_taxonomy_tags(9, market_taxonomy)
        assert tags == ["morning"]
        
        # Test afternoon hour
        tags = enrichment_service._get_time_taxonomy_tags(15, market_taxonomy)
        assert tags == ["afternoon"]
        
        # Test evening hour
        tags = enrichment_service._get_time_taxonomy_tags(20, market_taxonomy)
        assert tags == ["evening"]

    def test_location_coordinates(self, enrichment_service):
        """Test location coordinate generation."""
        # Test hub location
        coords = enrichment_service._get_location_coordinates("trading_hub")
        assert coords == [39.8283, -98.5795]
        
        # Test node location
        coords = enrichment_service._get_location_coordinates("transmission_node")
        assert coords == [40.7128, -74.0060]
        
        # Test default location
        coords = enrichment_service._get_location_coordinates("unknown_location")
        assert coords == [39.8283, -98.5795]

    def test_location_region(self, enrichment_service):
        """Test location region generation."""
        # Test hub region
        region = enrichment_service._get_location_region("trading_hub")
        assert region == "hub_region"
        
        # Test node region
        region = enrichment_service._get_location_region("transmission_node")
        assert region == "node_region"
        
        # Test unknown region
        region = enrichment_service._get_location_region("unknown_location")
        assert region == "unknown_region"

    def test_location_timezone(self, enrichment_service):
        """Test location timezone generation."""
        # Test hub timezone
        tz = enrichment_service._get_location_timezone("trading_hub")
        assert tz == "America/Chicago"
        
        # Test node timezone
        tz = enrichment_service._get_location_timezone("transmission_node")
        assert tz == "America/New_York"
        
        # Test default timezone
        tz = enrichment_service._get_location_timezone("unknown_location")
        assert tz == "UTC"

    def test_completeness_score(self, enrichment_service):
        """Test completeness score calculation."""
        # Test complete data
        complete_data = {
            "event_id": "test-123",
            "occurred_at": 1234567890,
            "tenant_id": "default",
            "market": "CAISO",
            "data_type": "trade"
        }
        score = enrichment_service._calculate_completeness_score(complete_data)
        assert score == 1.0
        
        # Test incomplete data
        incomplete_data = {
            "event_id": "test-456",
            "market": "MISO"
        }
        score = enrichment_service._calculate_completeness_score(incomplete_data)
        assert score == 0.4  # 2 out of 5 fields present

    def test_accuracy_score(self, enrichment_service):
        """Test accuracy score calculation."""
        # Test accurate data
        accurate_data = {
            "price": 50.0,
            "quantity": 100.0
        }
        score = enrichment_service._calculate_accuracy_score(accurate_data)
        assert score == 1.0
        
        # Test inaccurate data (negative price)
        inaccurate_data = {
            "price": -10.0,
            "quantity": 100.0
        }
        score = enrichment_service._calculate_accuracy_score(inaccurate_data)
        assert score == 0.8  # Deducted for negative price
        
        # Test inaccurate data (excessive quantity)
        excessive_data = {
            "price": 50.0,
            "quantity": 15000.0
        }
        score = enrichment_service._calculate_accuracy_score(excessive_data)
        assert score == 0.8  # Deducted for excessive quantity

    def test_consistency_score(self, enrichment_service):
        """Test consistency score calculation."""
        # Test consistent data
        consistent_data = {
            "price": "50.0",
            "quantity": "100.0"
        }
        score = enrichment_service._calculate_consistency_score(consistent_data)
        assert score == 1.0
        
        # Test inconsistent data (non-numeric)
        inconsistent_data = {
            "price": "invalid",
            "quantity": "also_invalid"
        }
        score = enrichment_service._calculate_consistency_score(inconsistent_data)
        assert score == 0.7  # Deducted for non-numeric values

    def test_market_segment(self, enrichment_service):
        """Test market segment generation."""
        # Test MISO market
        miso_data = {"market": "MISO", "data_type": "trade"}
        segment = enrichment_service._get_market_segment(miso_data)
        assert segment == "midwest_energy_market"
        
        # Test CAISO market
        caiso_data = {"market": "CAISO", "data_type": "curve"}
        segment = enrichment_service._get_market_segment(caiso_data)
        assert segment == "california_energy_market"
        
        # Test unknown market
        unknown_data = {"market": "UNKNOWN", "data_type": "unknown"}
        segment = enrichment_service._get_market_segment(unknown_data)
        assert segment == "unknown_market"

    def test_trading_session(self, enrichment_service):
        """Test trading session generation."""
        # Test peak session
        peak_data = {"delivery_hour": 8}
        session = enrichment_service._get_trading_session(peak_data)
        assert session == "peak_session"
        
        # Test off-peak session
        off_peak_data = {"delivery_hour": 14}
        session = enrichment_service._get_trading_session(off_peak_data)
        assert session == "off_peak_session"
        
        # Test unknown session
        unknown_data = {}
        session = enrichment_service._get_trading_session(unknown_data)
        assert session == "unknown_session"

    def test_price_tier(self, enrichment_service):
        """Test price tier generation."""
        # Test high tier
        high_data = {"price": 150.0}
        tier = enrichment_service._get_price_tier(high_data)
        assert tier == "high_tier"
        
        # Test mid tier
        mid_data = {"price": 75.0}
        tier = enrichment_service._get_price_tier(mid_data)
        assert tier == "mid_tier"
        
        # Test low tier
        low_data = {"price": 15.0}
        tier = enrichment_service._get_price_tier(low_data)
        assert tier == "low_tier"
        
        # Test unknown tier
        unknown_data = {}
        tier = enrichment_service._get_price_tier(unknown_data)
        assert tier == "unknown_tier"

    def test_day_of_week(self, enrichment_service):
        """Test day of week generation."""
        # Test valid date
        day = enrichment_service._get_day_of_week("2025-01-15")
        assert day == "Wednesday"
        
        # Test invalid date
        day = enrichment_service._get_day_of_week("invalid-date")
        assert day is None

    def test_month(self, enrichment_service):
        """Test month generation."""
        # Test valid date
        month = enrichment_service._get_month("2025-01-15")
        assert month == "January"
        
        # Test invalid date
        month = enrichment_service._get_month("invalid-date")
        assert month is None

    def test_quarter(self, enrichment_service):
        """Test quarter generation."""
        # Test Q1
        quarter = enrichment_service._get_quarter("2025-01-15")
        assert quarter == "Q1"
        
        # Test Q2
        quarter = enrichment_service._get_quarter("2025-05-15")
        assert quarter == "Q2"
        
        # Test Q3
        quarter = enrichment_service._get_quarter("2025-08-15")
        assert quarter == "Q3"
        
        # Test Q4
        quarter = enrichment_service._get_quarter("2025-11-15")
        assert quarter == "Q4"
        
        # Test invalid date
        quarter = enrichment_service._get_quarter("invalid-date")
        assert quarter is None

    def test_enrichment_score_calculation(self, enrichment_service):
        """Test enrichment score calculation."""
        # Test comprehensive enrichment
        comprehensive_data = {
            "taxonomy_tags": ["energy_trading", "spot_pricing", "medium_price"],
            "semantic_tags": ["california_energy", "normal_price", "daytime"],
            "geospatial_data": {"location": "hub", "coordinates": [39.8283, -98.5795]},
            "metadata_tags": {"data_quality": {"completeness": 1.0}}
        }
        score = enrichment_service._calculate_enrichment_score(comprehensive_data)
        assert score == 1.1  # Bonus for comprehensive enrichment
        
        # Test minimal enrichment
        minimal_data = {
            "taxonomy_tags": [],
            "semantic_tags": [],
            "geospatial_data": None,
            "metadata_tags": {}
        }
        score = enrichment_service._calculate_enrichment_score(minimal_data)
        assert score == 0.0  # No enrichment

    def test_get_stats(self, enrichment_service):
        """Test statistics retrieval."""
        stats = enrichment_service.get_stats()
        
        assert "total_enrichments" in stats
        assert "successful_enrichments" in stats
        assert "failed_enrichments" in stats
        assert "average_enrichment_score" in stats
        assert "success_rate" in stats

    def test_reset_stats(self, enrichment_service):
        """Test statistics reset."""
        # Add some stats
        enrichment_service.stats["total_enrichments"] = 10
        enrichment_service.stats["successful_enrichments"] = 8
        
        # Reset stats
        enrichment_service.reset_stats()
        
        # Verify reset
        assert enrichment_service.stats["total_enrichments"] == 0
        assert enrichment_service.stats["successful_enrichments"] == 0
        assert enrichment_service.stats["failed_enrichments"] == 0
        assert enrichment_service.stats["enrichment_scores"] == []
