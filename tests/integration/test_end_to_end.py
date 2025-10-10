"""
End-to-end tests for the complete data pipeline.
"""

import pytest
import asyncio
import json
import sys
from pathlib import Path
from unittest.mock import Mock, AsyncMock, patch
from datetime import datetime, timezone, timedelta
from uuid import uuid4

ROOT_DIR = Path(__file__).resolve().parents[3]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from connectors.caiso.connector import CAISOConnector
from connectors.caiso.config import CAISOConnectorConfig
from connectors.base import ExtractionResult, TransformationResult
from ingestion.tests.utils.import_helpers import import_from_services

EnrichmentService, = import_from_services(
    "services.service-enrichment.src.core.enricher",
    ["EnrichmentService"]
)
AggregationService, = import_from_services(
    "services.service-aggregation.src.core.aggregator",
    ["AggregationService"]
)


class TestEndToEndPipeline:
    """Test complete end-to-end data pipeline."""

    @pytest.fixture
    def caiso_connector(self):
        """Create CAISO connector."""
        config = CAISOConnectorConfig(
            name="test_caiso",
            version="1.0.0",
            market="CAISO",
            mode="batch",
            enabled=True,
            output_topic="ingestion.caiso.raw.v1",
            schema="schemas/raw_caiso_trade.avsc",
            retries=3,
            backoff_seconds=30,
            tenant_strategy="single",
            transforms=["sanitize_numeric", "standardize_timezone"],
            owner="platform",
            caiso_base_url="https://oasis.caiso.com/oasisapi",
            caiso_timeout=30,
            default_market_run_id="DAM",
            default_node="TH_SP15_GEN-APND"
        )
        return CAISOConnector(config)

    @pytest.fixture
    def enrichment_service(self):
        """Create enrichment service."""
        with patch('services.service_enrichment.src.core.enricher.TaxonomyService') as mock_taxonomy:
            mock_taxonomy.return_value.get_market_taxonomy.return_value = {
                "instruments": {
                    "market_price": ["spot_pricing", "real_time_market", "energy_trading"],
                    "trade": ["energy_trading", "physical_settlement", "executed_transaction"]
                },
                "locations": {
                    "hub": ["trading_hub", "high_liquidity", "price_discovery"],
                    "node": ["transmission_node", "medium_liquidity", "delivery_point"]
                },
                "price_ranges": {
                    "low": {"min": 0, "max": 50, "tags": ["low_price", "affordable_energy"]},
                    "medium": {"min": 50, "max": 100, "tags": ["medium_price", "moderate_energy"]},
                    "high": {"min": 100, "max": 1000, "tags": ["high_price", "expensive_energy"]}
                },
                "quantity_ranges": {
                    "small": {"min": 0, "max": 500, "tags": ["small_quantity", "light_volume"]},
                    "medium": {"min": 500, "max": 1000, "tags": ["medium_quantity", "moderate_volume"]},
                    "large": {"min": 1000, "max": 10000, "tags": ["large_quantity", "substantial_volume"]}
                },
                "time_periods": {
                    "off_peak": {"start": 0, "end": 6, "tags": ["off_peak", "low_demand", "night_time"]},
                    "morning": {"start": 6, "end": 12, "tags": ["morning", "rising_demand", "day_time"]},
                    "afternoon": {"start": 12, "end": 18, "tags": ["afternoon", "peak_demand", "day_time"]},
                    "evening": {"start": 18, "end": 24, "tags": ["evening", "declining_demand", "night_time"]}
                }
            }
            return EnrichmentService()

    @pytest.fixture
    def aggregation_service(self):
        """Create aggregation service."""
        with patch('services.service_aggregation.src.core.aggregator.open') as mock_open:
            mock_open.return_value.__enter__.return_value.read.return_value = """
ohlc_policies:
  daily:
    enabled: true
    aggregation_window: "1D"
    buffer_size: 1000
    flush_interval: "1H"
    min_trades: 1
    max_trades: 10000
  hourly:
    enabled: true
    aggregation_window: "1H"
    buffer_size: 100
    flush_interval: "15M"
    min_trades: 1
    max_trades: 1000
rolling_metrics:
  price_metrics:
    rolling_average_price:
      enabled: true
      window_sizes: [5, 10, 20, 50]
      min_samples: 3
      max_samples: 1000
    rolling_price_volatility:
      enabled: true
      window_sizes: [5, 10, 20, 50]
      min_samples: 5
      max_samples: 1000
curve_prestage:
  trade_curves:
    enabled: true
    buffer_size: 1000
    flush_interval: "1H"
    min_points: 5
    max_points: 10000
"""
            return AggregationService()

    @pytest.fixture
    def synthetic_caiso_data(self):
        """Generate synthetic CAISO data."""
        base_time = datetime.now(timezone.utc)
        data = []
        
        for i in range(24):  # 24 hours of data
            hour = i
            price = 50.0 + (i * 2.0) + (i % 3) * 5.0  # Varying prices
            quantity = 200.0 + (i * 10.0)  # Varying quantities
            
            record = {
                "event_id": str(uuid4()),
                "trace_id": str(uuid4()),
                "occurred_at": int((base_time + timedelta(hours=i)).timestamp() * 1_000_000),
                "tenant_id": "default",
                "schema_version": "1.0.0",
                "producer": "caiso-connector",
                "market": "CAISO",
                "market_id": "CAISO",
                "timezone": "UTC",
                "currency": "USD",
                "unit": "MWh",
                "price_unit": "$/MWh",
                "data_type": "market_price",
                "source": "caiso-oasis",
                "trade_id": None,
                "delivery_location": "TH_SP15_GEN-APND",
                "delivery_date": base_time.strftime("%Y-%m-%d"),
                "delivery_hour": hour,
                "price": price,
                "quantity": quantity,
                "bid_price": price - 1.0,
                "offer_price": price + 1.0,
                "clearing_price": price,
                "congestion_price": price * 0.1,
                "loss_price": price * 0.05,
                "curve_type": None,
                "price_type": "LMP",
                "status_type": None,
                "status_value": None,
                "timestamp": (base_time + timedelta(hours=i)).isoformat(),
                "raw_data": json.dumps([{
                    "INTERVALSTARTTIME_GMT": (base_time + timedelta(hours=i)).strftime("%Y-%m-%dT%H:%M:00-0000"),
                    "INTERVALENDTIME_GMT": (base_time + timedelta(hours=i+1)).strftime("%Y-%m-%dT%H:%M:00-0000"),
                    "NODE": "TH_SP15_GEN-APND",
                    "MARKET_RUN_ID": "DAM",
                    "LMP_TYPE": "LMP",
                    "MW": str(quantity),
                    "VALUE": str(price)
                }]),
                "transformation_timestamp": datetime.now(timezone.utc).isoformat()
            }
            data.append(record)
        
        return data

    @pytest.mark.asyncio
    async def test_complete_pipeline_flow(self, caiso_connector, enrichment_service, aggregation_service, synthetic_caiso_data):
        """Test complete pipeline from raw data to aggregated output."""
        
        # Step 1: Simulate CAISO extraction
        extraction_result = ExtractionResult(
            data=synthetic_caiso_data,
            metadata={
                "query": {
                    "start": "2025-01-15T00:00:00-0000",
                    "end": "2025-01-16T00:00:00-0000",
                    "market_run_id": "DAM",
                    "node": "TH_SP15_GEN-APND",
                    "version": "12"
                },
                "source": "oasis-singlezip",
                "records": len(synthetic_caiso_data)
            },
            record_count=len(synthetic_caiso_data)
        )
        
        assert extraction_result.record_count == 24
        assert len(extraction_result.data) == 24
        
        # Step 2: Simulate CAISO transformation
        with patch.object(caiso_connector._transformer, 'transform') as mock_transform:
            mock_transform.return_value = TransformationResult(
                data=synthetic_caiso_data,
                metadata={
                    "transformation_time": datetime.now(timezone.utc).isoformat(),
                    "config": {},
                    "input_record_count": 24,
                    "output_record_count": 24,
                    "validation_errors_count": 0,
                    "transforms_applied": ["sanitize_numeric", "standardize_timezone"]
                },
                record_count=24,
                validation_errors=[]
            )
            
            transformation_result = await caiso_connector.transform(extraction_result)
            
            assert transformation_result.record_count == 24
            assert len(transformation_result.validation_errors) == 0
        
        # Step 3: Enrichment processing
        enriched_results = []
        for record in synthetic_caiso_data:
            enrichment_result = await enrichment_service.enrich(record)
            enriched_results.append(enrichment_result)
            
            # Verify enrichment
            assert enrichment_result.enrichment_score > 0.0
            assert "taxonomy_tags" in enrichment_result.enriched_data
            assert "semantic_tags" in enrichment_result.enriched_data
            assert "geospatial_data" in enrichment_result.enriched_data
            assert "metadata_tags" in enrichment_result.enriched_data
        
        assert len(enriched_results) == 24
        
        # Step 4: Aggregation processing
        aggregation_results = []
        for enrichment_result in enriched_results:
            aggregation_result = await aggregation_service.aggregate(enrichment_result.enriched_data)
            aggregation_results.append(aggregation_result)
            
            # Verify aggregation
            assert hasattr(aggregation_result, 'ohlc_bars')
            assert hasattr(aggregation_result, 'rolling_metrics')
            assert hasattr(aggregation_result, 'curve_prestage')
        
        assert len(aggregation_results) == 24
        
        # Step 5: Verify pipeline outputs
        
        # Check OHLC bars
        ohlc_bars = []
        for result in aggregation_results:
            ohlc_bars.extend(result.ohlc_bars)
        
        assert len(ohlc_bars) > 0
        
        # Should have daily bars
        daily_bars = [bar for bar in ohlc_bars if bar.bar_type == "daily"]
        assert len(daily_bars) >= 1
        
        # Should have hourly bars
        hourly_bars = [bar for bar in ohlc_bars if bar.bar_type == "hourly"]
        assert len(hourly_bars) >= 1
        
        # Verify OHLC bar properties
        for bar in ohlc_bars:
            assert bar.market == "CAISO"
            assert bar.delivery_location == "TH_SP15_GEN-APND"
            assert bar.price_currency == "USD"
            assert bar.quantity_unit == "MWh"
            assert bar.open_price > 0
            assert bar.high_price >= bar.open_price
            assert bar.low_price <= bar.open_price
            assert bar.close_price > 0
            assert bar.volume > 0
            assert bar.trade_count > 0
        
        # Check rolling metrics
        rolling_metrics = []
        for result in aggregation_results:
            rolling_metrics.extend(result.rolling_metrics)
        
        # Should have some rolling metrics after enough data
        if len(rolling_metrics) > 0:
            metric_types = set(metric.metric_type for metric in rolling_metrics)
            assert "rolling_average_price" in metric_types or "rolling_total_volume" in metric_types
        
        # Check curve pre-stage
        curve_prestage = []
        for result in aggregation_results:
            curve_prestage.extend(result.curve_prestage)
        
        assert len(curve_prestage) == 24  # One per input record
        
        # Verify curve pre-stage properties
        for curve in curve_prestage:
            assert curve.market == "CAISO"
            assert curve.curve_type == "spot_curve"  # Based on data_type "market_price"
            assert curve.price_currency == "USD"
            assert curve.quantity_unit == "MWh"
            assert curve.price > 0
            assert curve.quantity > 0
            assert "curve_metadata" in curve.dict()

    @pytest.mark.asyncio
    async def test_pipeline_with_edge_cases(self, enrichment_service, aggregation_service):
        """Test pipeline with edge cases and error conditions."""
        
        # Test with minimal data
        minimal_data = {
            "event_id": str(uuid4()),
            "occurred_at": int(datetime.now(timezone.utc).timestamp() * 1_000_000),
            "tenant_id": "default",
            "market": "MISO",
            "data_type": "trade",
            "delivery_location": "node",
            "delivery_date": "2025-01-15",
            "price": 25.0,
            "quantity": 100.0
        }
        
        # Enrichment should handle minimal data
        enrichment_result = await enrichment_service.enrich(minimal_data)
        assert enrichment_result.enrichment_score >= 0.0
        
        # Aggregation should handle minimal data
        aggregation_result = await aggregation_service.aggregate(enrichment_result.enriched_data)
        assert hasattr(aggregation_result, 'ohlc_bars')
        assert hasattr(aggregation_result, 'rolling_metrics')
        assert hasattr(aggregation_result, 'curve_prestage')
        
        # Test with missing required fields
        incomplete_data = {
            "event_id": str(uuid4()),
            "market": "CAISO"
            # Missing required fields
        }
        
        # Enrichment should handle incomplete data gracefully
        enrichment_result = await enrichment_service.enrich(incomplete_data)
        assert enrichment_result.enrichment_score >= 0.0
        
        # Aggregation should handle incomplete data gracefully
        aggregation_result = await aggregation_service.aggregate(enrichment_result.enriched_data)
        assert hasattr(aggregation_result, 'ohlc_bars')
        assert hasattr(aggregation_result, 'rolling_metrics')
        assert hasattr(aggregation_result, 'curve_prestage')

    @pytest.mark.asyncio
    async def test_pipeline_performance(self, enrichment_service, aggregation_service, synthetic_caiso_data):
        """Test pipeline performance with larger datasets."""
        
        # Generate larger dataset
        large_dataset = []
        base_time = datetime.now(timezone.utc)
        
        for day in range(7):  # 7 days
            for hour in range(24):  # 24 hours per day
                for minute in range(0, 60, 15):  # Every 15 minutes
                    price = 50.0 + (day * 5.0) + (hour * 2.0) + (minute * 0.1)
                    quantity = 200.0 + (day * 10.0) + (hour * 5.0) + (minute * 0.5)
                    
                    record = {
                        "event_id": str(uuid4()),
                        "occurred_at": int((base_time + timedelta(days=day, hours=hour, minutes=minute)).timestamp() * 1_000_000),
                        "tenant_id": "default",
                        "market": "CAISO",
                        "data_type": "market_price",
                        "delivery_location": "TH_SP15_GEN-APND",
                        "delivery_date": (base_time + timedelta(days=day)).strftime("%Y-%m-%d"),
                        "delivery_hour": hour,
                        "price": price,
                        "quantity": quantity,
                        "source": "caiso-oasis"
                    }
                    large_dataset.append(record)
        
        # Process large dataset
        start_time = datetime.now(timezone.utc)
        
        enriched_results = []
        for record in large_dataset:
            enrichment_result = await enrichment_service.enrich(record)
            enriched_results.append(enrichment_result)
        
        enrichment_time = datetime.now(timezone.utc)
        enrichment_duration = (enrichment_time - start_time).total_seconds()
        
        aggregation_results = []
        for enrichment_result in enriched_results:
            aggregation_result = await aggregation_service.aggregate(enrichment_result.enriched_data)
            aggregation_results.append(aggregation_result)
        
        aggregation_time = datetime.now(timezone.utc)
        aggregation_duration = (aggregation_time - enrichment_time).total_seconds()
        total_duration = (aggregation_time - start_time).total_seconds()
        
        # Verify performance
        assert len(enriched_results) == len(large_dataset)
        assert len(aggregation_results) == len(large_dataset)
        
        # Performance assertions (adjust thresholds as needed)
        assert enrichment_duration < 10.0  # Enrichment should complete within 10 seconds
        assert aggregation_duration < 15.0  # Aggregation should complete within 15 seconds
        assert total_duration < 25.0  # Total pipeline should complete within 25 seconds
        
        # Verify data quality
        for enrichment_result in enriched_results:
            assert enrichment_result.enrichment_score > 0.0
        
        # Verify aggregation outputs
        total_ohlc_bars = sum(len(result.ohlc_bars) for result in aggregation_results)
        total_rolling_metrics = sum(len(result.rolling_metrics) for result in aggregation_results)
        total_curve_prestage = sum(len(result.curve_prestage) for result in aggregation_results)
        
        assert total_ohlc_bars > 0
        assert total_curve_prestage == len(large_dataset)  # One curve point per record

    @pytest.mark.asyncio
    async def test_pipeline_data_consistency(self, enrichment_service, aggregation_service, synthetic_caiso_data):
        """Test data consistency across pipeline stages."""
        
        # Process data through pipeline
        enriched_results = []
        for record in synthetic_caiso_data:
            enrichment_result = await enrichment_service.enrich(record)
            enriched_results.append(enrichment_result)
        
        aggregation_results = []
        for enrichment_result in enriched_results:
            aggregation_result = await aggregation_service.aggregate(enrichment_result.enriched_data)
            aggregation_results.append(aggregation_result)
        
        # Verify data consistency
        
        # 1. Event IDs should be preserved
        for i, (original, enriched, aggregated) in enumerate(zip(synthetic_caiso_data, enriched_results, aggregation_results)):
            assert original["event_id"] == enriched.enriched_data["event_id"]
            # Aggregation doesn't preserve event_id directly, but should be traceable through metadata
        
        # 2. Market information should be consistent
        for enriched_result in enriched_results:
            assert enriched_result.enriched_data["market"] == "CAISO"
        
        # 3. Price and quantity should be preserved
        for i, (original, enriched) in enumerate(zip(synthetic_caiso_data, enriched_results)):
            assert original["price"] == enriched.enriched_data["price"]
            assert original["quantity"] == enriched.enriched_data["quantity"]
        
        # 4. OHLC bars should reflect input data
        ohlc_bars = []
        for result in aggregation_results:
            ohlc_bars.extend(result.ohlc_bars)
        
        # Verify OHLC calculations
        for bar in ohlc_bars:
            if bar.bar_type == "daily":
                # Daily bar should aggregate all hours for the day
                assert bar.trade_count >= 1
                assert bar.volume > 0
                assert bar.open_price > 0
                assert bar.high_price >= bar.low_price
                assert bar.close_price > 0
        
        # 5. Enrichment scores should be reasonable
        enrichment_scores = [result.enrichment_score for result in enriched_results]
        assert all(score >= 0.0 for score in enrichment_scores)
        assert all(score <= 1.0 for score in enrichment_scores)
        assert sum(enrichment_scores) / len(enrichment_scores) > 0.5  # Average score should be reasonable

    @pytest.mark.asyncio
    async def test_pipeline_error_recovery(self, enrichment_service, aggregation_service):
        """Test pipeline error recovery and resilience."""
        
        # Test with malformed data
        malformed_data = {
            "event_id": str(uuid4()),
            "occurred_at": int(datetime.now(timezone.utc).timestamp() * 1_000_000),
            "tenant_id": "default",
            "market": "CAISO",
            "data_type": "market_price",
            "delivery_location": "hub",
            "delivery_date": "2025-01-15",
            "delivery_hour": 14,
            "price": "invalid_price",  # Invalid price
            "quantity": "invalid_quantity"  # Invalid quantity
        }
        
        # Enrichment should handle malformed data gracefully
        enrichment_result = await enrichment_service.enrich(malformed_data)
        assert enrichment_result.enrichment_score >= 0.0
        
        # Aggregation should handle malformed data gracefully
        aggregation_result = await aggregation_service.aggregate(enrichment_result.enriched_data)
        assert hasattr(aggregation_result, 'ohlc_bars')
        assert hasattr(aggregation_result, 'rolling_metrics')
        assert hasattr(aggregation_result, 'curve_prestage')
        
        # Test with extreme values
        extreme_data = {
            "event_id": str(uuid4()),
            "occurred_at": int(datetime.now(timezone.utc).timestamp() * 1_000_000),
            "tenant_id": "default",
            "market": "CAISO",
            "data_type": "market_price",
            "delivery_location": "hub",
            "delivery_date": "2025-01-15",
            "delivery_hour": 14,
            "price": 10000.0,  # Very high price
            "quantity": 100000.0  # Very high quantity
        }
        
        # Pipeline should handle extreme values
        enrichment_result = await enrichment_service.enrich(extreme_data)
        assert enrichment_result.enrichment_score >= 0.0
        
        aggregation_result = await aggregation_service.aggregate(enrichment_result.enriched_data)
        assert hasattr(aggregation_result, 'ohlc_bars')
        assert hasattr(aggregation_result, 'rolling_metrics')
        assert hasattr(aggregation_result, 'curve_prestage')
