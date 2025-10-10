"""
Unit tests for the CSV → Kafka market tick producer.

These tests validate idempotency behaviour, successful publish flows,
and DLQ routing when serialization errors bubble up from the producer.
"""

from __future__ import annotations

from pathlib import Path
from typing import List, Tuple

import pytest
from confluent_kafka.serialization import SerializationError

try:
    from connectors.base.exceptions import ConnectorError
    from connectors.csv_market_ticks import producer as csv_producer
    from connectors.csv_market_ticks.producer import (
        CSVMarketTicksProducer,
        ProducerConfig,
        compute_hash_key,
    )
except ModuleNotFoundError:  # pragma: no cover - fallback for repo-root imports
    from ingestion.connectors.base.exceptions import ConnectorError  # type: ignore
    from ingestion.connectors.csv_market_ticks import producer as csv_producer  # type: ignore
    from ingestion.connectors.csv_market_ticks.producer import (  # type: ignore
        CSVMarketTicksProducer,
        ProducerConfig,
        compute_hash_key,
    )


class _FakeKafkaProducer:
    """Lightweight stub for KafkaProducerService used in unit tests."""

    instances: List["_FakeKafkaProducer"] = []

    def __init__(self, *args, **kwargs) -> None:  # noqa: D401 - signature kept for compatibility
        self.published: List[Tuple[dict, str | None]] = []
        self.raise_error: Exception | None = None
        _FakeKafkaProducer.instances.append(self)

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def publish_record(self, record: dict, schema_str: str | None = None) -> bool:
        if self.raise_error is not None:
            raise self.raise_error
        self.published.append((record, schema_str))
        return True


@pytest.fixture
def csv_ticks_producer(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> CSVMarketTicksProducer:
    """Provide a producer instance with stubbed Kafka services."""

    # Ensure clean slate across tests
    _FakeKafkaProducer.instances.clear()

    monkeypatch.setattr(csv_producer, "KafkaProducerService", _FakeKafkaProducer)

    csv_file = tmp_path / "ticks.csv"
    csv_file.write_text("symbol,timestamp,source_id\n", encoding="utf-8")

    config = ProducerConfig(
        bootstrap_servers="kafka:9092",
        schema_registry_url="http://localhost:8081",
        source_path=csv_file,
        output_topic="ingestion.market.ticks.raw.v1",
        dlq_topic="processing.deadletter.market.ticks.v1",
        poll_interval=0.01,
        follow=False,
        max_retries=0,
        backoff_seconds=0.0,
        max_backoff=0.0,
        tenant_id="test-tenant",
        producer_id="csv-market-ticks@test",
        default_market="TEST",
        dedup_window=10,
    )

    return CSVMarketTicksProducer(config)


def _make_row(symbol: str, timestamp: str, source_id: str, line: int = 1) -> dict:
    """Helper to build CSV row dictionaries mimicking DictReader output."""
    return {
        "symbol": symbol,
        "timestamp": timestamp,
        "source_id": source_id,
        "_csv_line_number": line,
        "price": "12.34",
    }


def test_build_event_produces_idempotent_payload(csv_ticks_producer: CSVMarketTicksProducer) -> None:
    """Ensure event_id determinism and duplicate suppression."""
    row = _make_row("NG-FUT", "2024-01-01T00:00:00Z", "ICE")
    event = csv_ticks_producer._build_event(row)
    assert event is not None

    expected_event_id = compute_hash_key("NG-FUT", "2024-01-01T00:00:00Z", "ICE")
    assert event["event_id"] == expected_event_id
    assert event["payload"]["checksum"] is not None
    assert event["payload"]["market"] == "TEST"

    # Second invocation with the same row should be skipped by idempotency cache
    duplicate = csv_ticks_producer._build_event(row)
    assert duplicate is None


@pytest.mark.asyncio
async def test_publish_with_retry_success(monkeypatch: pytest.MonkeyPatch, csv_ticks_producer: CSVMarketTicksProducer) -> None:
    """Verify happy-path publish stores the record on the raw producer."""

    async def immediate_retry(func, **kwargs):
        return await func()

    monkeypatch.setattr(csv_producer, "async_retry_with_backoff", immediate_retry)

    row = _make_row("PJM-DA", "2024-01-02T00:00:00Z", "ICE", line=2)
    event = csv_ticks_producer._build_event(row)
    assert event is not None

    result = await csv_ticks_producer._publish_with_retry(event)
    assert result is True

    raw_producer = _FakeKafkaProducer.instances[0]
    assert len(raw_producer.published) == 1
    published_event, schema = raw_producer.published[0]
    assert published_event["event_id"] == event["event_id"]
    assert schema == csv_ticks_producer._raw_schema


@pytest.mark.asyncio
async def test_publish_with_retry_routes_serialization_errors_to_dlq(
    monkeypatch: pytest.MonkeyPatch,
    csv_ticks_producer: CSVMarketTicksProducer,
) -> None:
    """ConnectorError with serialization cause triggers DLQ publish."""

    async def immediate_retry(func, **kwargs):
        return await func()

    monkeypatch.setattr(csv_producer, "async_retry_with_backoff", immediate_retry)

    raw_producer, dlq_producer = _FakeKafkaProducer.instances
    serialization_cause = SerializationError("schema mismatch")
    error = ConnectorError("unable to serialize record")
    error.__cause__ = serialization_cause
    raw_producer.raise_error = error

    row = _make_row("ERCOT-RT", "2024-01-03T00:00:00Z", "POLLER", line=3)
    event = csv_ticks_producer._build_event(row)
    assert event is not None

    result = await csv_ticks_producer._publish_with_retry(event)
    assert result is False

    # Original record should not be tracked as published
    assert len(raw_producer.published) == 0

    # DLQ producer receives the failure wrapper
    assert len(dlq_producer.published) == 1
    dlq_event, schema = dlq_producer.published[0]
    assert dlq_event["payload"]["failed_event_id"] == event["event_id"]
    assert dlq_event["payload"]["failed_topic"] == csv_ticks_producer.config.output_topic
    assert schema == csv_ticks_producer._dlq_schema
