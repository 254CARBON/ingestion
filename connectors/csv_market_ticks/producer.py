#!/usr/bin/env python3
"""
CSV → Kafka ingestion bridge for market tick data.

This utility tails (or replays) a CSV file and publishes each row to the
`ingestion.market.ticks.raw.v1` topic using the standard ingestion envelope.
Records that fail Avro serialization after retry/backoff are routed to the
`processing.deadletter.market.ticks.v1` DLQ.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import hashlib
import json
import os
import signal
import sys
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, AsyncGenerator, Dict, Optional
from uuid import uuid4

import structlog
from confluent_kafka.serialization import SerializationError

try:
    from avro.io import AvroTypeException  # type: ignore
except ImportError:  # pragma: no cover - optional dependency
    AvroTypeException = None  # type: ignore[assignment]

try:
    import httpx
except ImportError:  # pragma: no cover - optional dependency
    httpx = None  # type: ignore[assignment]

if httpx is not None:
    _orig_httpx_client_init = httpx.Client.__init__
    _orig_httpx_async_init = httpx.AsyncClient.__init__

    def _normalize_proxy(kwargs: Dict[str, Any]) -> Dict[str, Any]:
        """Translate deprecated `proxy` kwarg used by confluent client into `proxies` for modern httpx."""
        proxy = kwargs.pop("proxy", None)
        if proxy is not None:
            kwargs.setdefault("proxies", proxy)
        return kwargs

    def _patched_httpx_client_init(self, *args, **kwargs):
        kwargs = _normalize_proxy(kwargs)
        return _orig_httpx_client_init(self, *args, **kwargs)

    def _patched_httpx_async_init(self, *args, **kwargs):
        kwargs = _normalize_proxy(kwargs)
        return _orig_httpx_async_init(self, *args, **kwargs)

    httpx.Client.__init__ = _patched_httpx_client_init  # type: ignore[assignment]
    httpx.AsyncClient.__init__ = _patched_httpx_async_init  # type: ignore[assignment]

try:
    from ingestion.connectors.base.exceptions import ConnectorError
    from ingestion.connectors.base.kafka_producer import KafkaProducerService
    from ingestion.connectors.base.utils import async_retry_with_backoff, setup_logging
except ModuleNotFoundError:  # pragma: no cover - fallback when package root differs
    from connectors.base.exceptions import ConnectorError  # type: ignore
    from connectors.base.kafka_producer import KafkaProducerService  # type: ignore
    from connectors.base.utils import async_retry_with_backoff, setup_logging  # type: ignore


DEFAULT_RAW_TOPIC = "ingestion.market.ticks.raw.v1"
DEFAULT_DLQ_TOPIC = "processing.deadletter.market.ticks.v1"
RAW_SCHEMA_PATH = "specs/events/market/ingestion.market.ticks.raw.v1.avsc"
DLQ_SCHEMA_PATH = "specs/events/market/processing.deadletter.market.ticks.v1.avsc"


def resolve_repo_root(start: Path) -> Path:
    """Resolve repository root by locating the `specs` directory."""
    current = start.resolve()
    for parent in [current] + list(current.parents):
        if (parent / "specs").is_dir():
            return parent
    raise RuntimeError("Unable to locate repository root (missing specs directory)")


def to_microseconds(value: Optional[str], *, default: Optional[int] = None) -> int:
    """
    Convert various timestamp representations to epoch microseconds.

    Supports ISO-8601 strings (with or without timezone designator) and
    integer strings representing seconds/milliseconds/microseconds.
    """
    if value is None or str(value).strip() == "":
        if default is not None:
            return default
        raise ValueError("Empty timestamp")

    raw = str(value).strip()

    # Numeric epoch formats
    if raw.isdigit():
        epoch = int(raw)
        if len(raw) >= 16:  # microseconds already
            return epoch
        if len(raw) >= 13:  # milliseconds
            return epoch * 1000
        return epoch * 1_000_000  # assume seconds

    # Handle trailing Z
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"

    try:
        dt = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise ValueError(f"Unsupported timestamp format: {value}") from exc

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return int(dt.timestamp() * 1_000_000)


def compute_hash_key(*parts: Any) -> str:
    """Compute deterministic SHA-256 hash from provided parts."""
    joined = "|".join("" if part is None else str(part).strip() for part in parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def truncate(text: str, limit: int = 4096) -> str:
    """Truncate strings for safe logging/payload storage."""
    if len(text) <= limit:
        return text
    return f"{text[: limit - 3]}..."


@dataclass
class ProducerConfig:
    """Configuration for the CSV → Kafka producer."""

    bootstrap_servers: str
    schema_registry_url: str
    source_path: Path
    output_topic: str = DEFAULT_RAW_TOPIC
    dlq_topic: str = DEFAULT_DLQ_TOPIC
    poll_interval: float = 1.0
    follow: bool = True
    max_retries: int = 5
    backoff_seconds: float = 1.0
    max_backoff: float = 30.0
    tenant_id: str = "default"
    producer_id: str = "csv-market-ticks@0.1.0"
    schema_version: str = "1.0.0"
    dedup_window: int = 50000
    default_market: Optional[str] = None


class CSVMarketTicksProducer:
    """Stream CSV rows into Kafka raw tick topic with retries and DLQ routing."""

    def __init__(self, config: ProducerConfig):
        self.config = config
        self.repo_root = resolve_repo_root(Path(__file__))
        self.logger = setup_logging("CSVMarketTicksProducer").bind(
            source_path=str(config.source_path),
            output_topic=config.output_topic,
            dlq_topic=config.dlq_topic,
        )

        self._raw_schema = self._load_schema(RAW_SCHEMA_PATH)
        self._dlq_schema = self._load_schema(DLQ_SCHEMA_PATH)

        self._producer = KafkaProducerService(
            bootstrap_servers=config.bootstrap_servers,
            schema_registry_url=config.schema_registry_url,
            output_topic=config.output_topic,
            use_event_envelope=False,
            default_producer=config.producer_id,
        )
        self._dlq_producer = KafkaProducerService(
            bootstrap_servers=config.bootstrap_servers,
            schema_registry_url=config.schema_registry_url,
            output_topic=config.dlq_topic,
            use_event_envelope=False,
            default_producer=config.producer_id,
        )

        self._reader_file: Optional[Any] = None
        self._dict_reader: Optional[csv.DictReader] = None
        self._line_number: int = 0
        self._stop_event: asyncio.Event = asyncio.Event()
        self._dedup_order: deque[str] = deque()
        self._dedup_index: set[str] = set()

    def _load_schema(self, relative_path: str) -> str:
        schema_path = self.repo_root / relative_path
        if not schema_path.is_file():
            raise FileNotFoundError(f"Schema not found: {schema_path}")
        return schema_path.read_text(encoding="utf-8")

    async def start(self) -> None:
        """Start Kafka producers."""
        await self._producer.start()
        await self._dlq_producer.start()
        self.logger.info("Kafka producers started")

    async def stop(self) -> None:
        """Stop Kafka producers and release resources."""
        await self._producer.stop()
        await self._dlq_producer.stop()
        self._close_reader()
        self.logger.info("Producer shutdown complete")

    def request_stop(self) -> None:
        """Signal run loop to exit."""
        self._stop_event.set()

    def _close_reader(self) -> None:
        if self._reader_file:
            try:
                self._reader_file.close()
            except OSError:
                pass
        self._reader_file = None
        self._dict_reader = None
        self._line_number = 0

    def _ensure_reader(self) -> None:
        """Ensure CSV reader is initialized and header is available."""
        if self._dict_reader is not None:
            return

        if not self.config.source_path.is_file():
            raise FileNotFoundError(f"CSV source not found at {self.config.source_path}")

        file_handle = self.config.source_path.open("r", encoding="utf-8", newline="")
        reader = csv.DictReader(file_handle)
        if not reader.fieldnames:
            file_handle.close()
            raise ValueError("CSV file missing header row")

        self._reader_file = file_handle
        self._dict_reader = reader
        self._line_number = 0

        self.logger.info("Initialized CSV reader", columns=reader.fieldnames)

    async def _row_stream(self) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Asynchronously stream rows from the CSV file.

        Tail behaviour is implemented by sleeping/polling when EOF is reached
        while `follow` remains true.
        """
        while not self._stop_event.is_set():
            try:
                self._ensure_reader()
            except FileNotFoundError as err:
                self.logger.warning("CSV file unavailable, retrying", error=str(err))
                await asyncio.sleep(self.config.poll_interval)
                continue
            except ValueError as err:
                self.logger.warning("CSV header missing, waiting for data", error=str(err))
                await asyncio.sleep(self.config.poll_interval)
                self._close_reader()
                continue

            try:
                row = next(self._dict_reader)  # type: ignore[assignment]
            except StopIteration:
                if not self.config.follow:
                    break
                await asyncio.sleep(self.config.poll_interval)
                try:
                    # Detect truncation/rotation by comparing current position
                    if self._reader_file and self._reader_file.tell() > self.config.source_path.stat().st_size:
                        self.logger.info("Detected file truncation, reopening source")
                        self._close_reader()
                except OSError:
                    self._close_reader()
                continue

            if row is None:
                continue

            self._line_number += 1
            cleaned = {
                key: (value.strip() if isinstance(value, str) else value)
                for key, value in row.items()
                if key is not None
            }

            if all((value is None or value == "") for value in cleaned.values()):
                continue

            cleaned["_csv_line_number"] = self._line_number
            yield cleaned

        self.logger.info("Row stream terminated")

    def _remember_idempotency(self, key: str) -> bool:
        """Track idempotency keys using sliding window."""
        if key in self._dedup_index:
            return False

        self._dedup_order.append(key)
        self._dedup_index.add(key)

        if len(self._dedup_order) > self.config.dedup_window > 0:
            oldest = self._dedup_order.popleft()
            self._dedup_index.discard(oldest)

        return True

    def _build_event(self, row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Transform CSV row into ingestion event."""
        normalized: Dict[str, Any] = {}
        for key, value in row.items():
            if key is None:
                continue
            if isinstance(key, str) and key.startswith("_"):
                continue
            normalized[key.lower()] = value

        symbol = normalized.get("symbol") or normalized.get("ticker")
        timestamp_raw = (
            normalized.get("timestamp")
            or normalized.get("ts")
            or normalized.get("occurred_at")
            or normalized.get("trade_time")
        )
        source_id = (
            normalized.get("source_id")
            or normalized.get("source_system")
            or normalized.get("source")
        )
        market = (
            normalized.get("market")
            or normalized.get("hub")
            or self.config.default_market
            or "UNKNOWN"
        )

        if not symbol or not timestamp_raw or not source_id:
            self.logger.warning(
                "Skipping row missing required fields",
                line=row.get("_csv_line_number"),
                symbol=symbol,
                timestamp=timestamp_raw,
                source_id=source_id,
            )
            return None

        try:
            occurred_at = to_microseconds(str(timestamp_raw))
        except ValueError:
            occurred_at = int(datetime.now(timezone.utc).timestamp() * 1_000_000)
            self.logger.warning(
                "Falling back to ingestion timestamp for occurred_at",
                line=row.get("_csv_line_number"),
                timestamp=timestamp_raw,
            )

        ingested_at = int(datetime.now(timezone.utc).timestamp() * 1_000_000)

        received_at_raw = (
            normalized.get("received_at")
            or normalized.get("ingested_at")
            or normalized.get("capture_time")
        )
        try:
            received_at = to_microseconds(received_at_raw, default=ingested_at)
        except ValueError:
            received_at = ingested_at

        sequence_raw = (
            normalized.get("sequence")
            or normalized.get("seq")
            or normalized.get("offset")
        )
        try:
            sequence = int(sequence_raw) if sequence_raw not in (None, "") else None
        except (TypeError, ValueError):
            sequence = None

        retry_count_raw = normalized.get("retry_count") or normalized.get("retries")
        try:
            retry_count = int(retry_count_raw) if retry_count_raw not in (None, "") else 0
        except (TypeError, ValueError):
            retry_count = 0

        idempotency_key = compute_hash_key(symbol, timestamp_raw, source_id)
        if not self._remember_idempotency(idempotency_key):
            self.logger.debug(
                "Duplicate row skipped by idempotency cache",
                event_id=idempotency_key,
                line=row.get("_csv_line_number"),
            )
            return None

        raw_payload_map = {
            key: value
            for key, value in row.items()
            if isinstance(key, str) and not key.startswith("_")
        }
        raw_payload = json.dumps(raw_payload_map, sort_keys=True, default=str)
        checksum = hashlib.sha256(raw_payload.encode("utf-8")).hexdigest()

        metadata_keys = {"symbol", "ticker", "timestamp", "ts", "occurred_at", "trade_time",
                         "source_id", "source_system", "source", "market", "hub", "sequence",
                         "seq", "offset", "retry_count", "retries", "received_at", "ingested_at",
                         "capture_time"}
        metadata: Dict[str, str] = {}
        for key, value in raw_payload_map.items():
            lowered = key.lower()
            if lowered in metadata_keys or value in (None, ""):
                continue
            metadata[key] = str(value)
        metadata["csv_line_number"] = str(row.get("_csv_line_number", ""))
        metadata["csv_source_path"] = str(self.config.source_path)

        event = {
            "event_id": idempotency_key,
            "trace_id": str(uuid4()),
            "schema_version": self.config.schema_version,
            "tenant_id": self.config.tenant_id,
            "producer": self.config.producer_id,
            "occurred_at": occurred_at,
            "ingested_at": ingested_at,
            "payload": {
                "source_system": str(source_id),
                "market": str(market),
                "symbol": str(symbol),
                "sequence": sequence,
                "received_at": received_at,
                "raw_payload": raw_payload,
                "encoding": "csv-line",
                "metadata": metadata,
                "retry_count": retry_count,
                "checksum": checksum,
            },
        }

        return event

    async def _publish_with_retry(self, event: Dict[str, Any]) -> bool:
        """Publish event with exponential backoff retries and DLQ fallback."""

        async def _send() -> bool:
            return await self._producer.publish_record(event, schema_str=self._raw_schema)

        try:
            await async_retry_with_backoff(
                _send,
                max_retries=self.config.max_retries,
                backoff_seconds=self.config.backoff_seconds,
                max_backoff=self.config.max_backoff,
                exceptions=(ConnectorError,),
            )
            self.logger.info(
                "Published market tick",
                event_id=event["event_id"],
                symbol=event["payload"]["symbol"],
                market=event["payload"]["market"],
            )
            return True
        except ConnectorError as exc:
            if self._is_serialization_error(exc):
                self.logger.error(
                    "Serialization failure, routing to DLQ",
                    event_id=event["event_id"],
                    error=str(exc),
                )
                await self._publish_dlq(event, exc)
                return False

            self.logger.error(
                "Failed to publish event after retries",
                event_id=event["event_id"],
                error=str(exc),
            )
            return False

    async def _publish_dlq(self, event: Dict[str, Any], error: Exception) -> None:
        """Publish serialization failures to the DLQ topic."""
        now = int(datetime.now(timezone.utc).timestamp() * 1_000_000)
        cause = getattr(error, "__cause__", None)
        error_message = truncate(str(cause or error), limit=2048)

        metadata = {
            "exception_type": cause.__class__.__name__ if cause else error.__class__.__name__,
            "source_path": str(self.config.source_path),
            "producer": self.config.producer_id,
        }

        dlq_event = {
            "event_id": str(uuid4()),
            "trace_id": event.get("trace_id", str(uuid4())),
            "schema_version": self.config.schema_version,
            "tenant_id": self.config.tenant_id,
            "producer": self.config.producer_id,
            "occurred_at": now,
            "ingested_at": now,
            "payload": {
                "failed_event_id": event.get("event_id", ""),
                "failed_topic": self.config.output_topic,
                "failure_stage": "INGESTION",
                "error_code": "SERIALIZATION_ERROR",
                "error_message": error_message,
                "retry_count": self.config.max_retries,
                "last_retry_at": now,
                "raw_event": truncate(json.dumps(event, sort_keys=True, default=str), limit=8192),
                "normalized_event": None,
                "metadata": {key: str(value) for key, value in metadata.items()},
            },
        }

        async def _send_dlq() -> bool:
            return await self._dlq_producer.publish_record(dlq_event, schema_str=self._dlq_schema)

        try:
            await async_retry_with_backoff(
                _send_dlq,
                max_retries=self.config.max_retries,
                backoff_seconds=self.config.backoff_seconds,
                max_backoff=self.config.max_backoff,
                exceptions=(ConnectorError,),
            )
            self.logger.info(
                "Serialized failure event routed to DLQ",
                failed_event_id=event.get("event_id"),
                dlq_event_id=dlq_event["event_id"],
            )
        except ConnectorError as dlq_error:
            self.logger.error(
                "Failed to publish DLQ event",
                failed_event_id=event.get("event_id"),
                error=str(dlq_error),
            )

    @staticmethod
    def _is_serialization_error(exc: ConnectorError) -> bool:
        """Heuristic detection of serialization-related failures."""
        checked = set()
        cause = getattr(exc, "__cause__", None)

        while cause and cause not in checked:
            checked.add(cause)
            if isinstance(cause, SerializationError):
                return True
            if AvroTypeException and isinstance(cause, AvroTypeException):
                return True
            if isinstance(cause, (ValueError, TypeError)):
                message = str(cause).lower()
                if any(token in message for token in ("avro", "schema", "field", "type", "required")):
                    return True
            cause = getattr(cause, "__cause__", None)

        message = str(exc).lower()
        return "serialization" in message or "avro" in message

    async def run(self) -> None:
        """Entry point for the producer."""
        await self.start()
        try:
            async for row in self._row_stream():
                if self._stop_event.is_set():
                    break
                event = self._build_event(row)
                if event is None:
                    continue
                await self._publish_with_retry(event)
        finally:
            await self.stop()


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="CSV market tick ingestion producer")
    parser.add_argument(
        "--source",
        type=Path,
        default=os.environ.get("DATA_PROC_SOURCE_PATH"),
        help="Path to source CSV file (env: DATA_PROC_SOURCE_PATH)",
    )
    parser.add_argument(
        "--bootstrap",
        default=os.environ.get("DATA_PROC_KAFKA_BOOTSTRAP", "localhost:9092"),
        help="Kafka bootstrap servers (env: DATA_PROC_KAFKA_BOOTSTRAP)",
    )
    parser.add_argument(
        "--schema-registry",
        default=os.environ.get("DATA_PROC_SCHEMA_REGISTRY", "http://localhost:8081"),
        help="Schema Registry URL (env: DATA_PROC_SCHEMA_REGISTRY)",
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=float(os.environ.get("DATA_PROC_POLL_INTERVAL", "1.0")),
        help="Polling interval (seconds) when tailing the CSV file",
    )
    parser.add_argument(
        "--mode",
        choices=("follow", "replay"),
        default=os.environ.get("DATA_PROC_MODE", "follow"),
        help="follow keeps tailing, replay exits after reaching EOF",
    )
    parser.add_argument(
        "--output-topic",
        default=os.environ.get("DATA_PROC_OUTPUT_TOPIC", DEFAULT_RAW_TOPIC),
        help=f"Kafka topic for raw ticks (default: {DEFAULT_RAW_TOPIC})",
    )
    parser.add_argument(
        "--dlq-topic",
        default=os.environ.get("DATA_PROC_DLQ_TOPIC", DEFAULT_DLQ_TOPIC),
        help=f"Kafka DLQ topic (default: {DEFAULT_DLQ_TOPIC})",
    )
    parser.add_argument(
        "--tenant-id",
        default=os.environ.get("DATA_PROC_TENANT_ID", "default"),
        help="Tenant identifier applied to events",
    )
    parser.add_argument(
        "--producer-id",
        default=os.environ.get("DATA_PROC_PRODUCER_ID", "csv-market-ticks@0.1.0"),
        help="Producer identifier string applied to events",
    )
    parser.add_argument(
        "--default-market",
        default=os.environ.get("DATA_PROC_DEFAULT_MARKET"),
        help="Default market when CSV rows omit the market column",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=int(os.environ.get("DATA_PROC_MAX_RETRIES", "5")),
        help="Number of publish retry attempts before failing",
    )
    parser.add_argument(
        "--backoff",
        type=float,
        default=float(os.environ.get("DATA_PROC_BACKOFF_SECONDS", "1.0")),
        help="Base seconds for exponential backoff",
    )
    parser.add_argument(
        "--max-backoff",
        type=float,
        default=float(os.environ.get("DATA_PROC_MAX_BACKOFF", "30.0")),
        help="Maximum backoff delay in seconds",
    )
    parser.add_argument(
        "--dedup-window",
        type=int,
        default=int(os.environ.get("DATA_PROC_DEDUP_WINDOW", "50000")),
        help="Number of recent idempotency keys to track for duplicate suppression",
    )
    return parser.parse_args(argv)


def build_config(args: argparse.Namespace) -> ProducerConfig:
    """Build strongly-typed configuration from CLI arguments."""
    source_path = args.source
    if source_path is None:
        raise ValueError("CSV source path must be provided via --source or DATA_PROC_SOURCE_PATH")

    follow_mode = args.mode == "follow"

    return ProducerConfig(
        bootstrap_servers=args.bootstrap,
        schema_registry_url=args.schema_registry,
        source_path=Path(source_path),
        output_topic=args.output_topic,
        dlq_topic=args.dlq_topic,
        poll_interval=args.poll_interval,
        follow=follow_mode,
        max_retries=args.max_retries,
        backoff_seconds=args.backoff,
        max_backoff=args.max_backoff,
        tenant_id=args.tenant_id,
        producer_id=args.producer_id,
        default_market=args.default_market,
        dedup_window=args.dedup_window,
    )


async def main_async(config: ProducerConfig) -> None:
    """Async entry wrapper installing signal handlers."""
    producer = CSVMarketTicksProducer(config)
    loop = asyncio.get_running_loop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, producer.request_stop)
        except NotImplementedError:
            # add_signal_handler is not available on some platforms (e.g., Windows)
            signal.signal(sig, lambda *_: producer.request_stop())

    await producer.run()


def main(argv: Optional[list[str]] = None) -> int:
    """CLI entry point."""
    try:
        args = parse_args(argv)
        config = build_config(args)
    except Exception as exc:  # pragma: no cover - CLI argument validation
        print(f"[ERROR] {exc}", file=sys.stderr)
        return 2

    try:
        asyncio.run(main_async(config))
    except KeyboardInterrupt:
        return 130
    except Exception as exc:  # pragma: no cover - top-level guard
        print(f"[ERROR] Producer run failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
