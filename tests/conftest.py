"""
Pytest configuration and fixtures for 254Carbon Ingestion Platform tests.
"""

import asyncio
import os
import sys
import tempfile
from pathlib import Path
from typing import Dict, Any, List
from unittest.mock import Mock, AsyncMock

import pytest

import structlog
from pydantic import BaseModel

# Ensure ingestion root is on sys.path for imports
TESTS_ROOT = Path(__file__).resolve().parent
INGESTION_ROOT = TESTS_ROOT.parent
if str(INGESTION_ROOT) not in sys.path:
    sys.path.insert(0, str(INGESTION_ROOT))

# Configure structured logging for tests
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer()
    ],
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)


@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        yield Path(tmp_dir)


@pytest.fixture
def sample_connector_config():
    """Sample connector configuration for testing."""
    return {
        "name": "test_connector",
        "version": "1.0.0",
        "market": "TEST",
        "mode": "batch",
        "schedule": "0 0 * * *",
        "enabled": True,
        "output_topic": "ingestion.test.raw.v1",
        "schema": "schemas/raw_test_trade.avsc",
        "retries": 3,
        "backoff_seconds": 30,
        "tenant_strategy": "single",
        "transforms": ["sanitize_numeric", "standardize_timezone"],
        "owner": "platform",
        "description": "Test connector for unit tests",
        "tags": ["test", "unit"]
    }


@pytest.fixture
def sample_raw_record():
    """Sample raw record for testing."""
    return {
        "event_id": "test-event-123",
        "trace_id": "test-trace-456",
        "occurred_at": 1640995200000000,  # 2022-01-01 00:00:00 UTC
        "tenant_id": "test-tenant",
        "schema_version": "1.0.0",
        "producer": "test-connector",
        "trade_id": "test-trade-789",
        "settlement_point": "TEST_HUB",
        "price": 50.25,
        "quantity": 100.0,
        "trade_date": "2022-01-01",
        "trade_hour": 12,
        "trade_type": "DAM",
        "product_type": "Energy",
        "raw_data": '{"test": "data"}',
        "extraction_metadata": {
            "extraction_time": "2022-01-01T00:00:00Z",
            "source": "test-api",
            "version": "1.0.0"
        },
        "market": "MISO"
    }


@pytest.fixture
def sample_normalized_record():
    """Sample normalized record for testing."""
    return {
        "event_id": "test-event-123",
        "trace_id": "test-trace-456",
        "occurred_at": 1640995200000000,
        "tenant_id": "test-tenant",
        "schema_version": "1.0.0",
        "producer": "test-connector",
        "market": "TEST",
        "instrument_id": "TEST_test-trade-789",
        "instrument_type": "Energy",
        "delivery_location": "TEST_HUB",
        "delivery_date": "2022-01-01",
        "delivery_hour": 12,
        "price": 50.25,
        "quantity": 100.0,
        "trade_type": "DAM",
        "product_type": "Energy",
        "currency": "USD",
        "unit": "MWh",
        "price_unit": "$/MWh",
        "quality_flags": [],
        "validation_errors": [],
        "normalization_metadata": {
            "normalized_at": "2022-01-01T00:00:00Z",
            "market": "TEST",
            "version": "1.0.0"
        }
    }


@pytest.fixture
def sample_enriched_record():
    """Sample enriched record for testing."""
    base_record = {
        "event_id": "test-event-123",
        "trace_id": "test-trace-456",
        "occurred_at": 1640995200000000,
        "tenant_id": "test-tenant",
        "schema_version": "1.0.0",
        "producer": "test-connector",
        "market": "TEST",
        "instrument_id": "TEST_test-trade-789",
        "instrument_type": "Energy",
        "delivery_location": "TEST_HUB",
        "delivery_date": "2022-01-01",
        "delivery_hour": 12,
        "price": 50.25,
        "quantity": 100.0,
        "trade_type": "DAM",
        "product_type": "Energy",
        "currency": "USD",
        "unit": "MWh",
        "price_unit": "$/MWh",
        "quality_flags": [],
        "validation_errors": [],
        "semantic_tags": ["energy", "day-ahead", "test"],
        "taxonomy_class": "Energy",
        "taxonomy_subclass": "DayAhead",
        "market_segment": "Power",
        "geographic_region": "TestRegion",
        "time_of_day_category": "Peak",
        "seasonal_category": "Winter",
        "volatility_category": "Medium",
        "liquidity_category": "High",
        "enrichment_metadata": {
            "enriched_at": "2022-01-01T00:00:00Z",
            "taxonomy_version": "1.0.0",
            "enrichment_version": "1.0.0"
        }
    }
    return base_record


@pytest.fixture
def mock_kafka_producer():
    """Mock Kafka producer for testing."""
    producer = Mock()
    producer.send = AsyncMock()
    producer.flush = AsyncMock()
    producer.close = AsyncMock()
    return producer


@pytest.fixture
def mock_kafka_consumer():
    """Mock Kafka consumer for testing."""
    consumer = Mock()
    consumer.subscribe = AsyncMock()
    consumer.poll = AsyncMock()
    consumer.commit = AsyncMock()
    consumer.close = AsyncMock()
    return consumer


@pytest.fixture
def mock_http_client():
    """Mock HTTP client for testing."""
    client = Mock()
    client.get = AsyncMock()
    client.post = AsyncMock()
    client.put = AsyncMock()
    client.delete = AsyncMock()
    return client


@pytest.fixture
def mock_connector_registry():
    """Mock connector registry service for testing."""
    registry = Mock()
    registry.get_connector = Mock()
    registry.get_enabled_connectors = Mock()
    registry.validate_connector = Mock()
    registry.get_health_status = AsyncMock()
    return registry


@pytest.fixture
def mock_normalization_service():
    """Mock normalization service for testing."""
    service = Mock()
    service.normalize_record = AsyncMock()
    service.normalize_batch = AsyncMock()
    service.get_health_status = AsyncMock()
    return service


@pytest.fixture
def mock_enrichment_service():
    """Mock enrichment service for testing."""
    service = Mock()
    service.enrich_record = AsyncMock()
    service.enrich_batch = AsyncMock()
    service.get_health_status = AsyncMock()
    return service


@pytest.fixture
def mock_aggregation_service():
    """Mock aggregation service for testing."""
    service = Mock()
    service.aggregate_records = AsyncMock()
    service.get_health_status = AsyncMock()
    return service


@pytest.fixture
def sample_avro_schema():
    """Sample Avro schema for testing."""
    return {
        "type": "record",
        "name": "TestRecord",
        "namespace": "com.254carbon.test",
        "fields": [
            {
                "name": "event_id",
                "type": "string"
            },
            {
                "name": "occurred_at",
                "type": "long"
            },
            {
                "name": "price",
                "type": ["null", "double"],
                "default": None
            },
            {
                "name": "quantity",
                "type": ["null", "double"],
                "default": None
            }
        ]
    }


@pytest.fixture
def test_data_dir(temp_dir):
    """Create a test data directory with sample files."""
    data_dir = temp_dir / "test_data"
    data_dir.mkdir()
    
    # Create sample files
    (data_dir / "sample.json").write_text('{"test": "data"}')
    (data_dir / "sample.csv").write_text("col1,col2\nval1,val2")
    
    return data_dir


@pytest.fixture
def test_schemas_dir(temp_dir):
    """Create a test schemas directory with sample schemas."""
    schemas_dir = temp_dir / "schemas"
    schemas_dir.mkdir()
    
    # Create sample schema file
    schema_content = """{
        "type": "record",
        "name": "TestSchema",
        "fields": [
            {"name": "id", "type": "string"},
            {"name": "value", "type": "double"}
        ]
    }"""
    
    (schemas_dir / "test_schema.avsc").write_text(schema_content)
    
    return schemas_dir


# Test configuration
def pytest_configure(config):
    """Configure pytest with custom settings."""
    config.addinivalue_line(
        "markers", "unit: mark test as a unit test"
    )
    config.addinivalue_line(
        "markers", "integration: mark test as an integration test"
    )
    config.addinivalue_line(
        "markers", "slow: mark test as slow running"
    )


# Test collection hooks
def pytest_collection_modifyitems(config, items):
    """Modify test collection to add markers."""
    for item in items:
        # Add unit marker to tests in unit/ directory
        if "unit" in str(item.fspath):
            item.add_marker(pytest.mark.unit)
        
        # Add integration marker to tests in integration/ directory
        if "integration" in str(item.fspath):
            item.add_marker(pytest.mark.integration)
        
        # Add slow marker to tests with "slow" in the name
        if "slow" in item.name:
            item.add_marker(pytest.mark.slow)
