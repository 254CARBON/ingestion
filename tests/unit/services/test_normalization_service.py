"""
Unit tests for normalization service.
"""

import pytest
import asyncio
from unittest.mock import Mock, AsyncMock, patch
from datetime import datetime, timezone

from services.service_normalization.src.core.normalizer import NormalizationService
from services.service_normalization.src.core.rules_engine import RulesEngine
from services.service_normalization.src.core.validators import ValidationService
from connectors.base import ExtractionResult, TransformationResult


class TestNormalizationService:
    """Test normalization service."""
    
    @pytest.fixture
    def normalization_service(self):
        """Create normalization service."""
        return NormalizationService()
    
    @pytest.mark.asyncio
    async def test_get_health_status(self, normalization_service):
        """Test health status."""
        status = await normalization_service.get_health_status()
        
        assert "status" in status
        assert "timestamp" in status
        assert "uptime_seconds" in status
        assert "processing_stats" in status
        assert "errors" in status
    
    @pytest.mark.asyncio
    async def test_is_ready(self, normalization_service):
        """Test readiness check."""
        ready = await normalization_service.is_ready()
        assert isinstance(ready, bool)
    
    @pytest.mark.asyncio
    async def test_normalize_record_success(self, normalization_service, sample_raw_record):
        """Test successful record normalization."""
        result = await normalization_service.normalize_record(sample_raw_record)
        
        assert isinstance(result, dict)
        assert "event_id" in result
        assert "occurred_at" in result
        assert "market" in result
        assert "normalization_timestamp" in result
    
    @pytest.mark.asyncio
    async def test_normalize_record_failure(self, normalization_service):
        """Test record normalization failure."""
        result = await normalization_service.normalize_record(None)
        assert result == {}
    
    @pytest.mark.asyncio
    async def test_normalize_batch_success(self, normalization_service, sample_raw_record):
        """Test successful batch normalization."""
        raw_records = [sample_raw_record, sample_raw_record]
        
        result = await normalization_service.normalize_batch(raw_records)
        
        assert isinstance(result, list)
        assert len(result) == 2
        assert all("event_id" in record for record in result)
    
    @pytest.mark.asyncio
    async def test_normalize_batch_partial_failure(self, normalization_service, sample_raw_record):
        """Test batch normalization with partial failures."""
        # Create one valid and one invalid record
        invalid_record = {"invalid": "data"}
        raw_records = [sample_raw_record, invalid_record]
        
        result = await normalization_service.normalize_batch(raw_records)
        
        # Should return both records (valid ones normalized, invalid ones empty)
        assert isinstance(result, list)
        assert len(result) == 2
        # First record should be normalized
        assert "event_id" in result[0]
        # Second record should be empty due to failure
        assert result[1] == {}
    
    def test_extract_market(self, normalization_service):
        """Test market extraction."""
        # Test MISO market
        miso_record = {"producer": "miso-connector"}
        market = normalization_service._extract_market(miso_record)
        assert market == "MISO"
        
        # Test CAISO market
        caiso_record = {"producer": "caiso-connector"}
        market = normalization_service._extract_market(caiso_record)
        assert market == "CAISO"
        
        # Test unknown market
        unknown_record = {"producer": "unknown-connector"}
        market = normalization_service._extract_market(unknown_record)
        assert market is None
    
    def test_standardize_timestamp(self, normalization_service):
        """Test timestamp standardization."""
        # Test integer timestamp
        timestamp = 1640995200000000
        result = normalization_service._standardize_timestamp(timestamp)
        assert result == timestamp
        
        # Test string timestamp
        timestamp_str = "2022-01-01T00:00:00Z"
        result = normalization_service._standardize_timestamp(timestamp_str)
        assert isinstance(result, int)
        assert result > 0
        
        # Test datetime object
        dt = datetime(2022, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        result = normalization_service._standardize_timestamp(dt)
        assert isinstance(result, int)
        assert result > 0
        
        # Test invalid timestamp
        result = normalization_service._standardize_timestamp("invalid")
        assert isinstance(result, int)
        assert result > 0
    
    @pytest.mark.asyncio
    async def test_standardize_instrument_fields(self, normalization_service, sample_raw_record):
        """Test instrument field standardization."""
        result = await normalization_service._standardize_instrument_fields(sample_raw_record, "TEST")
        
        assert "instrument_id" in result
        assert "instrument_type" in result
        assert "delivery_location" in result
        assert "delivery_date" in result
        assert "delivery_hour" in result
        
        assert result["instrument_id"].startswith("TEST_")
        assert result["instrument_type"] == "Energy"
    
    @pytest.mark.asyncio
    async def test_standardize_price_fields(self, normalization_service, sample_raw_record):
        """Test price field standardization."""
        result = await normalization_service._standardize_price_fields(sample_raw_record)
        
        assert "price" in result
        assert "quantity" in result
        
        # Test numeric conversion
        if result["price"] is not None:
            assert isinstance(result["price"], float)
        if result["quantity"] is not None:
            assert isinstance(result["quantity"], float)
    
    def test_generate_quality_flags(self, normalization_service, sample_raw_record):
        """Test quality flag generation."""
        flags = normalization_service._generate_quality_flags(sample_raw_record)
        
        assert isinstance(flags, list)
        # Should have no flags for a valid record
        assert len(flags) == 0
        
        # Test with invalid record
        invalid_record = {"invalid": "data"}
        flags = normalization_service._generate_quality_flags(invalid_record)
        assert isinstance(flags, list)
        assert len(flags) > 0
    
    @pytest.mark.asyncio
    async def test_get_processing_metrics(self, normalization_service):
        """Test processing metrics."""
        metrics = await normalization_service.get_processing_metrics()
        
        assert "uptime_seconds" in metrics
        assert "total_records" in metrics
        assert "successful_records" in metrics
        assert "failed_records" in metrics
        assert "success_rate" in metrics
        assert "error_rate" in metrics
        assert "throughput_per_second" in metrics
    
    @pytest.mark.asyncio
    async def test_get_throughput_metrics(self, normalization_service):
        """Test throughput metrics."""
        metrics = await normalization_service.get_throughput_metrics()
        
        assert "records_per_second" in metrics
        assert "successful_per_second" in metrics
        assert "failed_per_second" in metrics
        assert "total_records" in metrics
        assert "uptime_seconds" in metrics
    
    @pytest.mark.asyncio
    async def test_get_latency_metrics(self, normalization_service):
        """Test latency metrics."""
        metrics = await normalization_service.get_latency_metrics()
        
        assert "p50_latency_ms" in metrics
        assert "p95_latency_ms" in metrics
        assert "p99_latency_ms" in metrics
        assert "max_latency_ms" in metrics
        assert "avg_latency_ms" in metrics
    
    @pytest.mark.asyncio
    async def test_get_error_metrics(self, normalization_service):
        """Test error metrics."""
        metrics = await normalization_service.get_error_metrics()
        
        assert "total_errors" in metrics
        assert "validation_errors" in metrics
        assert "transformation_errors" in metrics
        assert "error_rate" in metrics
        assert "error_types" in metrics


class TestRulesEngine:
    """Test rules engine."""
    
    @pytest.fixture
    def rules_engine(self):
        """Create rules engine."""
        return RulesEngine()
    
    @pytest.mark.asyncio
    async def test_get_health_status(self, rules_engine):
        """Test health status."""
        status = await rules_engine.get_health_status()
        
        assert "status" in status
        assert "timestamp" in status
        assert "uptime_seconds" in status
        assert "rules_loaded" in status
        assert "errors" in status
    
    @pytest.mark.asyncio
    async def test_is_ready(self, rules_engine):
        """Test readiness check."""
        ready = await rules_engine.is_ready()
        assert isinstance(ready, bool)
    
    @pytest.mark.asyncio
    async def test_get_metrics(self, rules_engine):
        """Test metrics."""
        metrics = await rules_engine.get_metrics()
        
        assert "uptime_seconds" in metrics
        assert "rules_loaded" in metrics
        assert "rules_applied" in metrics
        assert "rules_failed" in metrics
        assert "timestamp" in metrics


class TestValidationService:
    """Test validation service."""
    
    @pytest.fixture
    def validation_service(self):
        """Create validation service."""
        return ValidationService()
    
    @pytest.mark.asyncio
    async def test_get_health_status(self, validation_service):
        """Test health status."""
        status = await validation_service.get_health_status()
        
        assert "status" in status
        assert "timestamp" in status
        assert "uptime_seconds" in status
        assert "validation_stats" in status
        assert "errors" in status
    
    @pytest.mark.asyncio
    async def test_is_ready(self, validation_service):
        """Test readiness check."""
        ready = await validation_service.is_ready()
        assert ready is True
    
    @pytest.mark.asyncio
    async def test_get_metrics(self, validation_service):
        """Test metrics."""
        metrics = await validation_service.get_metrics()
        
        assert "uptime_seconds" in metrics
        assert "total_validations" in metrics
        assert "successful_validations" in metrics
        assert "failed_validations" in metrics
        assert "validation_errors" in metrics
        assert "success_rate" in metrics
        assert "error_rate" in metrics
        assert "timestamp" in metrics
    
    @pytest.mark.asyncio
    async def test_get_error_metrics(self, validation_service):
        """Test error metrics."""
        metrics = await validation_service.get_error_metrics()
        
        assert "total_errors" in metrics
        assert "error_rate" in metrics
        assert "error_types" in metrics
        assert "validation" in metrics["error_types"]
