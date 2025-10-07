"""
Unit tests for normalization service.
"""

import pytest
import asyncio
from unittest.mock import Mock, AsyncMock, patch
from datetime import datetime, timezone

from services.service-normalization.src.core.normalizer import NormalizationService, NormalizationResult
from services.service-normalization.src.core.rules_engine import RulesEngine
from services.service-normalization.src.core.validators import ValidationService, ValidationResult


class TestNormalizationService:
    """Test normalization service."""

    @pytest.fixture
    def normalization_service(self):
        """Create normalization service."""
        with patch('services.service-normalization.src.core.normalizer.open') as mock_open:
            mock_open.return_value.__enter__.return_value.read.return_value = """
markets:
  CAISO:
    market_id: "CAISO"
    timezone: "America/Los_Angeles"
    currency: "USD"
    unit: "MWh"
    price_unit: "$/MWh"
    field_mappings:
      node: "delivery_location"
      trade_date: "delivery_date"
      trade_hour: "delivery_hour"
    transforms:
      - name: "sanitize_numeric"
        fields: ["price", "quantity", "bid_price", "offer_price"]
      - name: "standardize_timezone"
        target_timezone: "UTC"
      - name: "validate_required_fields"
        required_fields: ["event_id", "occurred_at", "tenant_id"]
  MISO:
    market_id: "MISO"
    timezone: "America/Chicago"
    currency: "USD"
    unit: "MWh"
    price_unit: "$/MWh"
    field_mappings:
      settlement_point: "delivery_location"
      trade_date: "delivery_date"
      trade_hour: "delivery_hour"
    transforms:
      - name: "sanitize_numeric"
        fields: ["price", "quantity"]
      - name: "standardize_timezone"
        target_timezone: "UTC"
global:
  default_timezone: "UTC"
  default_currency: "USD"
  default_unit: "MWh"
  default_price_unit: "$/MWh"
  quality_thresholds:
    completeness: 0.95
    accuracy: 0.90
    consistency: 0.95
"""
            return NormalizationService()

    @pytest.fixture
    def sample_raw_data(self):
        """Sample raw data."""
        return {
            "event_id": "test-event-123",
            "occurred_at": int(datetime.now(timezone.utc).timestamp() * 1_000_000),
            "tenant_id": "default",
            "schema_version": "1.0.0",
            "producer": "test-producer",
            "node": "TH_SP15_GEN-APND",
            "trade_date": "2025-01-15",
            "trade_hour": 14,
            "price": "75.50",
            "quantity": "250.0",
            "bid_price": "74.50",
            "offer_price": "76.50"
        }

    @pytest.mark.asyncio
    async def test_normalize_success(self, normalization_service, sample_raw_data):
        """Test successful normalization."""
        result = await normalization_service.normalize(sample_raw_data)

        assert isinstance(result, NormalizationResult)
        assert result.normalized_data["event_id"] == sample_raw_data["event_id"]
        assert result.normalized_data["market_id"] == "CAISO"
        assert result.normalized_data["delivery_location"] == "TH_SP15_GEN-APND"
        assert result.normalized_data["delivery_date"] == "2025-01-15"
        assert result.normalized_data["delivery_hour"] == 14
        assert isinstance(result.normalized_data["price"], float)
        assert isinstance(result.normalized_data["quantity"], float)
        assert result.quality_score > 0.0

    @pytest.mark.asyncio
    async def test_normalize_caiso_data(self, normalization_service, sample_raw_data):
        """Test CAISO data normalization."""
        result = await normalization_service.normalize(sample_raw_data)

        assert result.normalized_data["market_id"] == "CAISO"
        assert result.normalized_data["timezone"] == "America/Los_Angeles"
        assert result.normalized_data["currency"] == "USD"
        assert result.normalized_data["unit"] == "MWh"
        assert result.normalized_data["price_unit"] == "$/MWh"

    @pytest.mark.asyncio
    async def test_normalize_miso_data(self, normalization_service):
        """Test MISO data normalization."""
        miso_data = {
            "event_id": "test-event-456",
            "occurred_at": int(datetime.now(timezone.utc).timestamp() * 1_000_000),
            "tenant_id": "default",
            "schema_version": "1.0.0",
            "producer": "test-producer",
            "settlement_point": "HUB.MISO",
            "trade_date": "2025-01-15",
            "trade_hour": 10,
            "price": "45.25",
            "quantity": "150.0"
        }

        result = await normalization_service.normalize(miso_data)

        assert result.normalized_data["market_id"] == "MISO"
        assert result.normalized_data["delivery_location"] == "HUB.MISO"
        assert result.normalized_data["timezone"] == "America/Chicago"

    @pytest.mark.asyncio
    async def test_normalize_missing_market(self, normalization_service):
        """Test normalization with missing market detection."""
        unknown_data = {
            "event_id": "test-event-789",
            "occurred_at": int(datetime.now(timezone.utc).timestamp() * 1_000_000),
            "tenant_id": "default",
            "schema_version": "1.0.0",
            "producer": "test-producer",
            "delivery_location": "UNKNOWN_HUB",
            "delivery_date": "2025-01-15",
            "price": "50.0"
        }

        with pytest.raises(ValueError, match="Unable to determine market"):
            await normalization_service.normalize(unknown_data)

    def test_extract_market(self, normalization_service):
        """Test market extraction."""
        # Test CAISO detection
        caiso_data = {"caiso": "data"}
        market = normalization_service._extract_market(caiso_data)
        assert market == "CAISO"

        # Test MISO detection
        miso_data = {"miso": "data"}
        market = normalization_service._extract_market(miso_data)
        assert market == "MISO"

        # Test direct market field
        direct_data = {"market": "TEST_MARKET"}
        market = normalization_service._extract_market(direct_data)
        assert market == "TEST_MARKET"

    def test_sanitize_numeric_fields(self, normalization_service):
        """Test numeric field sanitization."""
        data = {
            "price": "75.50",
            "quantity": "250.0",
            "bid_price": "$74.50",
            "invalid_price": "abc"
        }

        result = normalization_service._sanitize_numeric_fields(data, ["price", "quantity", "bid_price"])

        assert isinstance(result["price"], float)
        assert isinstance(result["quantity"], float)
        assert isinstance(result["bid_price"], float)
        assert result["invalid_price"] is None

    def test_standardize_timezone(self, normalization_service):
        """Test timezone standardization."""
        data = {
            "delivery_datetime": "2025-01-15T14:00:00",
            "occurred_at": int(datetime.now(timezone.utc).timestamp() * 1_000_000)
        }

        result = normalization_service._standardize_timezone(data, "UTC")

        assert result["delivery_datetime_timezone"] == "UTC"

    def test_validate_required_fields(self, normalization_service):
        """Test required field validation."""
        # Valid data
        valid_data = {
            "event_id": "test-123",
            "occurred_at": 1234567890,
            "tenant_id": "default"
        }
        result = normalization_service._validate_required_fields(valid_data, ["event_id", "occurred_at", "tenant_id"])
        assert "_validation_errors" not in result

        # Missing required fields
        invalid_data = {
            "event_id": "test-456"
            # Missing occurred_at and tenant_id
        }
        result = normalization_service._validate_required_fields(invalid_data, ["event_id", "occurred_at", "tenant_id"])
        assert "_validation_errors" in result

    def test_calculate_completeness(self, normalization_service):
        """Test completeness calculation."""
        # Complete market price data
        complete_data = {
            "market": "CAISO",
            "data_type": "market_price",
            "delivery_location": "hub",
            "delivery_date": "2025-01-15",
            "delivery_hour": 14,
            "price": 75.50
        }
        score = normalization_service._calculate_completeness(complete_data)
        assert score == 1.0

        # Incomplete data
        incomplete_data = {
            "market": "CAISO"
        }
        score = normalization_service._calculate_completeness(incomplete_data)
        assert score < 1.0

    def test_calculate_accuracy(self, normalization_service):
        """Test accuracy calculation."""
        # Accurate data
        accurate_data = {
            "price": 75.50,
            "quantity": 250.0,
            "occurred_at": int(datetime.now(timezone.utc).timestamp() * 1_000_000)
        }
        score = normalization_service._calculate_accuracy(accurate_data)
        assert score == 1.0

        # Inaccurate data (negative price)
        inaccurate_data = {
            "price": -10.0,
            "quantity": 250.0,
            "occurred_at": int(datetime.now(timezone.utc).timestamp() * 1_000_000)
        }
        score = normalization_service._calculate_accuracy(inaccurate_data)
        assert score < 1.0

        # Inaccurate data (excessive quantity)
        excessive_data = {
            "price": 75.50,
            "quantity": 15000.0,
            "occurred_at": int(datetime.now(timezone.utc).timestamp() * 1_000_000)
        }
        score = normalization_service._calculate_accuracy(excessive_data)
        assert score < 1.0

    def test_calculate_consistency(self, normalization_service):
        """Test consistency calculation."""
        # Consistent data
        consistent_data = {
            "data_type": "market_price",
            "market": "CAISO",
            "price": "75.50",
            "quantity": "250.0"
        }
        score = normalization_service._calculate_consistency(consistent_data)
        assert score == 1.0

        # Inconsistent data (non-numeric)
        inconsistent_data = {
            "data_type": "market_price",
            "market": "CAISO",
            "price": "invalid",
            "quantity": "also_invalid"
        }
        score = normalization_service._calculate_consistency(inconsistent_data)
        assert score < 1.0

    def test_calculate_timeliness(self, normalization_service):
        """Test timeliness calculation."""
        current_time = int(datetime.now(timezone.utc).timestamp() * 1_000_000)

        # Recent data (within 1 hour)
        recent_data = {
            "occurred_at": current_time - (3600 * 1_000_000)  # 1 hour ago
        }
        score = normalization_service._calculate_timeliness(recent_data)
        assert score == 1.0

        # Old data (more than 1 month)
        old_data = {
            "occurred_at": current_time - (86400 * 35 * 1_000_000)  # 35 days ago
        }
        score = normalization_service._calculate_timeliness(old_data)
        assert score < 0.5

    def test_calculate_validity(self, normalization_service):
        """Test validity calculation."""
        # Valid data
        valid_data = {
            "event_id": "test-123",
            "occurred_at": int(datetime.now(timezone.utc).timestamp() * 1_000_000),
            "tenant_id": "default",
            "schema_version": "1.0.0",
            "producer": "test-producer",
            "market": "CAISO",
            "data_type": "market_price",
            "delivery_location": "hub",
            "delivery_date": "2025-01-15",
            "price": 75.50
        }
        score = normalization_service._calculate_validity(valid_data)
        assert score == 1.0

        # Invalid data (missing envelope fields)
        invalid_data = {
            "market": "CAISO",
            "price": 75.50
        }
        score = normalization_service._calculate_validity(invalid_data)
        assert score < 1.0

    def test_quality_score_calculation(self, normalization_service):
        """Test overall quality score calculation."""
        # High quality data
        high_quality_data = {
            "market": "CAISO",
            "data_type": "market_price",
            "delivery_location": "hub",
            "delivery_date": "2025-01-15",
            "delivery_hour": 14,
            "price": 75.50,
            "quantity": 250.0,
            "event_id": "test-123",
            "occurred_at": int(datetime.now(timezone.utc).timestamp() * 1_000_000),
            "tenant_id": "default",
            "schema_version": "1.0.0",
            "producer": "test-producer"
        }

        # Mock validation result
        mock_validation = Mock()
        mock_validation.errors = []
        mock_validation.warnings = []

        score = normalization_service._calculate_quality_score(high_quality_data, mock_validation)
        assert score > 0.9  # Should be very high quality

    def test_get_stats(self, normalization_service):
        """Test statistics retrieval."""
        stats = normalization_service.get_stats()

        assert "total_processed" in stats
        assert "successful_normalizations" in stats
        assert "failed_normalizations" in stats
        assert "quality_scores" in stats
        assert "average_quality_score" in stats

    def test_reset_stats(self, normalization_service):
        """Test statistics reset."""
        # Add some stats
        normalization_service.stats["total_processed"] = 10
        normalization_service.stats["successful_normalizations"] = 8
        normalization_service.stats["quality_scores"] = [0.9, 0.8, 0.95]

        # Reset stats
        normalization_service.reset_stats()

        # Verify reset
        assert normalization_service.stats["total_processed"] == 0
        assert normalization_service.stats["successful_normalizations"] == 0
        assert normalization_service.stats["failed_normalizations"] == 0
        assert normalization_service.stats["quality_scores"] == []


class TestRulesEngine:
    """Test rules engine."""

    @pytest.fixture
    def rules_engine(self):
        """Create rules engine."""
        with patch('services.service-normalization.src.core.rules_engine.open') as mock_open:
            mock_open.return_value.__enter__.return_value.read.return_value = """
markets:
  CAISO:
    transforms:
      - name: "sanitize_numeric"
        fields: ["price"]
  MISO:
    transforms:
      - name: "sanitize_numeric"
        fields: ["price", "quantity"]
global:
  transformations:
    numeric_precision: 6
"""
            return RulesEngine()

    @pytest.mark.asyncio
    async def test_apply_rules_success(self, rules_engine):
        """Test successful rule application."""
        data = {
            "price": "75.50",
            "quantity": "250.0"
        }

        result = await rules_engine.apply_rules(data)

        assert isinstance(result, dict)
        assert "price" in result
        assert "quantity" in result

    @pytest.mark.asyncio
    async def test_rule_priority_ordering(self, rules_engine):
        """Test that rules are applied in priority order."""
        # The rules engine should apply rules in priority order
        # Higher priority rules should be applied first

        data = {"price": "75.50"}
        context = {"market": "CAISO"}

        result = await rules_engine.apply_rules(data, context)

        assert isinstance(result, dict)

    def test_evaluate_condition(self, rules_engine):
        """Test condition evaluation."""
        data = {"market": "CAISO", "price": 75.50}
        context = {}

        # Test simple condition
        condition = {"market": "CAISO"}
        result = rules_engine._evaluate_condition(condition, data, context)
        assert result is True

        # Test complex condition
        complex_condition = {"market": {"operator": "eq", "value": "CAISO"}}
        result = rules_engine._evaluate_condition(complex_condition, data, context)
        assert result is True

    def test_evaluate_complex_condition(self, rules_engine):
        """Test complex condition evaluation."""
        # Test equality
        condition = {"operator": "eq", "value": 75.50}
        result = rules_engine._evaluate_complex_condition(condition, 75.50)
        assert result is True

        # Test greater than
        condition = {"operator": "gt", "value": 70}
        result = rules_engine._evaluate_complex_condition(condition, 75.50)
        assert result is True

        # Test less than
        condition = {"operator": "lt", "value": 80}
        result = rules_engine._evaluate_complex_condition(condition, 75.50)
        assert result is True

    def test_values_equal(self, rules_engine):
        """Test value equality checking."""
        # Test exact equality
        assert rules_engine._values_equal(75.50, 75.50) is True
        assert rules_engine._values_equal("test", "test") is True

        # Test case-insensitive strings
        assert rules_engine._values_equal("CAISO", "caiso") is True

        # Test numeric tolerance
        assert rules_engine._values_equal(75.50, 75.501) is True  # Within tolerance

        # Test list equality
        assert rules_engine._values_equal([1, 2, 3], [1, 2, 3]) is True

        # Test inequality
        assert rules_engine._values_equal(75.50, 80.0) is False


class TestValidationService:
    """Test validation service."""

    @pytest.fixture
    def validation_service(self):
        """Create validation service."""
        with patch('services.service-normalization.src.core.validators.open') as mock_open:
            mock_open.return_value.__enter__.return_value.read.return_value = """
markets:
  CAISO:
    validation_rules:
      - name: "caiso_price_range"
        field: "price"
        type: "range"
        parameters: {"min": 0, "max": 2000}
global:
  validation:
    strict_mode: false
    check_required_fields: true
"""
            return ValidationService()

    @pytest.mark.asyncio
    async def test_validate_success(self, validation_service):
        """Test successful validation."""
        data = {
            "event_id": "test-123",
            "occurred_at": int(datetime.now(timezone.utc).timestamp() * 1_000_000),
            "tenant_id": "default",
            "schema_version": "1.0.0",
            "producer": "test-producer",
            "market": "CAISO",
            "price": 75.50
        }

        result = await validation_service.validate(data)

        assert isinstance(result, ValidationResult)
        assert result.status in ["valid", "warning"]
        assert result.score > 0.0

    @pytest.mark.asyncio
    async def test_validate_business_logic(self, validation_service):
        """Test business logic validation."""
        data = {
            "event_id": "test-123",
            "occurred_at": int(datetime.now(timezone.utc).timestamp() * 1_000_000),
            "tenant_id": "default",
            "schema_version": "1.0.0",
            "producer": "test-producer",
            "market": "CAISO",
            "price": 2500.0,  # Exceeds CAISO price cap
            "clearing_price": 2500.0
        }

        result = await validation_service.validate(data)

        assert isinstance(result, ValidationResult)
        assert len(result.warnings) > 0 or len(result.errors) > 0

    def test_calculate_validation_score(self, validation_service):
        """Test validation score calculation."""
        data = {"test": "data"}
        errors = []
        warnings = []

        score = validation_service._calculate_validation_score(data, errors, warnings)
        assert score == 1.0

        # Test with errors
        errors = ["Error 1", "Error 2"]
        score = validation_service._calculate_validation_score(data, errors, warnings)
        assert score < 1.0

        # Test with warnings
        errors = []
        warnings = ["Warning 1", "Warning 2"]
        score = validation_service._calculate_validation_score(data, errors, warnings)
        assert score < 1.0

    @pytest.mark.asyncio
    async def test_validate_field_rules(self, validation_service):
        """Test individual field validation rules."""
        # Test required field validation
        messages = await validation_service._validate_field("event_id", None, "required", {})
        assert len(messages) > 0
        assert "Required field" in messages[0]

        # Test range validation
        messages = await validation_service._validate_field("price", 2500, "range", {"min": 0, "max": 2000})
        assert len(messages) > 0
        assert "above maximum" in messages[0]

        # Test regex validation
        messages = await validation_service._validate_field("location", "INVALID_LOCATION", "regex", {"pattern": r"^[A-Z]{3}_[A-Z0-9]+_[A-Z]+$"})
        assert len(messages) > 0

    def test_is_valid_email(self, validation_service):
        """Test email validation."""
        assert validation_service._is_valid_email("test@example.com") is True
        assert validation_service._is_valid_email("invalid-email") is False
        assert validation_service._is_valid_email("") is False

    def test_is_valid_url(self, validation_service):
        """Test URL validation."""
        assert validation_service._is_valid_url("https://example.com") is True
        assert validation_service._is_valid_url("http://test.org") is True
        assert validation_service._is_valid_url("not-a-url") is False

    def test_matches_regex(self, validation_service):
        """Test regex matching."""
        assert validation_service._matches_regex("ABC123", r"^[A-Z0-9]+$") is True
        assert validation_service._matches_regex("abc123", r"^[A-Z0-9]+$") is False  # Case sensitive by default

    def test_is_valid_date(self, validation_service):
        """Test date validation."""
        assert validation_service._is_valid_date("2025-01-15", "%Y-%m-%d") is True
        assert validation_service._is_valid_date("invalid-date", "%Y-%m-%d") is False
        assert validation_service._is_valid_date("2025-01-15", "%m/%d/%Y") is False  # Wrong format