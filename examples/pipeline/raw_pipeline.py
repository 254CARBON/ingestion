"""
Exemplar ingestion pipeline that publishes raw events to Kafka.

This script orchestrates one or more connectors, executes extraction and
transformation, and publishes raw payloads to the configured Kafka topics.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
import time
from contextlib import suppress
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Type

import yaml

from connectors.base import BaseConnector, ConnectorConfig, LoadResult
from connectors.caiso import CAISOConnector, CAISOConnectorConfig

# Optional connector imports – best-effort
try:  # pragma: no cover - optional dependency
    from connectors.miso import MISOConnector, MISOConnectorConfig  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    MISOConnector = None  # type: ignore
    MISOConnectorConfig = None  # type: ignore


@dataclass
class ConnectorRunSummary:
    """Human-friendly summary for a single connector execution."""
    
    name: str
    market: str
    status: str
    extraction_count: int
    transformation_count: int
    load_published: int
    load_failed: int
    load_success: bool
    validation_errors: int
    duration_seconds: float
    output_topic: str
    load_errors: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None


class RawKafkaIngestionPipeline:
    """Coordinate connectors to publish raw events into Kafka topics."""
    
    CONNECTOR_REGISTRY: Dict[str, Tuple[Type[BaseConnector], Type[ConnectorConfig]]] = {
        "caiso": (CAISOConnector, CAISOConnectorConfig),
    }
    
    if MISOConnector and MISOConnectorConfig:  # pragma: no branch - optional
        CONNECTOR_REGISTRY["miso"] = (MISOConnector, MISOConnectorConfig)  # type: ignore[arg-type]
    
    def __init__(self, config: Dict[str, Any], config_dir: Path):
        self.config = config
        self.config_dir = config_dir
        self.kafka_defaults = config.get("kafka", {})
        self.pipeline_meta = config.get("pipeline", {})
        self.logger = logging.getLogger(self.__class__.__name__)
    
    @classmethod
    def from_path(cls, config_path: Path) -> "RawKafkaIngestionPipeline":
        """Create pipeline configuration from YAML path."""
        with config_path.open("r", encoding="utf-8") as handle:
            config_data = yaml.safe_load(handle) or {}
        return cls(config=config_data, config_dir=config_path.parent)
    
    async def run(
        self,
        *,
        connector_filter: Optional[List[str]] = None,
        dry_run: bool = False,
    ) -> List[ConnectorRunSummary]:
        """Run the pipeline for the selected connectors."""
        summaries: List[ConnectorRunSummary] = []
        connectors_cfg = self.config.get("connectors", [])
        
        if not connectors_cfg:
            self.logger.warning("No connectors configured in pipeline")
            return summaries
        
        for connector_entry in connectors_cfg:
            name = connector_entry.get("name") or connector_entry.get("type", "unknown")
            if connector_filter and name not in connector_filter:
                self.logger.debug("Skipping connector via filter", connector=name)
                continue
            
            summary = await self._run_connector(connector_entry, dry_run=dry_run)
            summaries.append(summary)
        
        return summaries
    
    async def _run_connector(
        self,
        connector_entry: Dict[str, Any],
        *,
        dry_run: bool,
    ) -> ConnectorRunSummary:
        """Execute a single connector from the configuration entry."""
        connector: Optional[BaseConnector] = None
        connector_config: Optional[ConnectorConfig] = None
        name = connector_entry.get("name") or connector_entry.get("type", "unknown")
        start_time = time.perf_counter()
        
        try:
            connector, connector_config = self._build_connector(connector_entry)
            extract_params = self._parse_extract_params(connector_entry)
            
            self.logger.info(
                "Running connector",
                extra={
                    "connector": connector_config.name,
                    "market": connector_config.market,
                    "output_topic": connector_config.output_topic,
                    "extract_params": self._serialize_extract_params(extract_params),
                },
            )
            
            extraction_result = await connector.extract(**extract_params)
            transformation_result = await connector.transform(extraction_result)
            
            if dry_run:
                load_result = LoadResult(
                    records_attempted=transformation_result.record_count,
                    records_published=0,
                    records_failed=0,
                    metadata={"load_method": "dry-run"},
                    errors=[],
                )
            else:
                load_result = await connector.load(transformation_result)
            
            duration = time.perf_counter() - start_time
            summary = ConnectorRunSummary(
                name=name,
                market=connector_config.market,
                status="success" if load_result.success else "partial",
                extraction_count=extraction_result.record_count,
                transformation_count=transformation_result.record_count,
                load_published=load_result.records_published,
                load_failed=load_result.records_failed,
                load_success=load_result.success,
                validation_errors=len(transformation_result.validation_errors),
                duration_seconds=duration,
                output_topic=connector_config.output_topic,
                load_errors=load_result.errors,
                metadata={
                    "pipeline": self.pipeline_meta,
                    "extract_params": self._serialize_extract_params(extract_params),
                    "load_metadata": load_result.metadata,
                },
            )
            return summary
        
        except Exception as exc:  # pragma: no cover - error path
            duration = time.perf_counter() - start_time
            self.logger.error("Connector run failed", connector=name, error=str(exc))
            return ConnectorRunSummary(
                name=name,
                market=getattr(connector_config, "market", "unknown") if connector_config else "unknown",
                status="failed",
                extraction_count=0,
                transformation_count=0,
                load_published=0,
                load_failed=0,
                load_success=False,
                validation_errors=0,
                duration_seconds=duration,
                output_topic=getattr(connector_config, "output_topic", "unknown") if connector_config else "unknown",
                load_errors=[str(exc)],
                metadata={"pipeline": self.pipeline_meta},
                error=str(exc),
            )
        
        finally:
            if connector:
                with suppress(Exception):
                    await connector.cleanup()
    
    def _build_connector(
        self,
        connector_entry: Dict[str, Any],
    ) -> Tuple[BaseConnector, ConnectorConfig]:
        """Instantiate connector and its configuration."""
        connector_type = connector_entry.get("type", "").lower()
        if connector_type not in self.CONNECTOR_REGISTRY:
            raise ValueError(f"Unsupported connector type: {connector_type}")
        
        connector_cls, config_cls = self.CONNECTOR_REGISTRY[connector_type]
        connector_config = self._build_connector_config(connector_entry, config_cls)
        connector = connector_cls(connector_config)
        return connector, connector_config
    
    def _build_connector_config(
        self,
        connector_entry: Dict[str, Any],
        config_cls: Type[ConnectorConfig],
    ) -> ConnectorConfig:
        """Load connector configuration from file and overrides."""
        config_block = connector_entry.get("connector_config", {})
        config_data: Dict[str, Any] = {}
        
        config_file = config_block.get("file")
        if config_file:
            config_path = self._resolve_path(config_file)
            with config_path.open("r", encoding="utf-8") as handle:
                loaded = yaml.safe_load(handle) or {}
                config_data.update(loaded)
        
        overrides = config_block.get("overrides", {})
        config_data.update(overrides)
        
        kafka_bootstrap = self.kafka_defaults.get("bootstrap_servers")
        if kafka_bootstrap and not config_data.get("kafka_bootstrap_servers"):
            config_data["kafka_bootstrap_servers"] = kafka_bootstrap
        
        schema_registry = self.kafka_defaults.get("schema_registry_url")
        if schema_registry and not config_data.get("schema_registry_url"):
            config_data["schema_registry_url"] = schema_registry
        
        return config_cls(**config_data)
    
    def _parse_extract_params(self, connector_entry: Dict[str, Any]) -> Dict[str, Any]:
        """Compute extraction parameters from configuration."""
        extract_block = connector_entry.get("extract", {})
        params = dict(extract_block.get("params", {}))
        now = datetime.now(timezone.utc)
        
        if "start" in params:
            params["start"] = self._parse_datetime(params["start"])
        else:
            lookback_hours = extract_block.get("lookback_hours", 1)
            start = now - timedelta(hours=lookback_hours)
            params["start"] = start
            params.setdefault("end", now)
        
        if "end" in params and isinstance(params["end"], str):
            params["end"] = self._parse_datetime(params["end"])
        elif "end" not in params:
            params["end"] = now
        
        return params
    
    def _parse_datetime(self, value: Any) -> datetime:
        """Parse supported datetime formats into timezone-aware UTC datetimes."""
        if isinstance(value, datetime):
            return value.astimezone(timezone.utc)
        
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(value, tz=timezone.utc)
        
        if isinstance(value, str):
            cleaned = value.strip()
            if cleaned.endswith("Z"):
                cleaned = cleaned[:-1] + "+00:00"
            try:
                return datetime.fromisoformat(cleaned).astimezone(timezone.utc)
            except ValueError as exc:
                raise ValueError(f"Unsupported datetime format: {value}") from exc
        
        raise ValueError(f"Unsupported datetime value: {value!r}")
    
    def _serialize_extract_params(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Convert parameters to JSON-serializable types."""
        serialized: Dict[str, Any] = {}
        for key, value in params.items():
            if isinstance(value, datetime):
                serialized[key] = value.isoformat()
            else:
                serialized[key] = value
        return serialized
    
    def _resolve_path(self, path_str: str) -> Path:
        """Resolve a path relative to the pipeline configuration."""
        path = Path(path_str)
        if path.is_absolute():
            return path
        
        candidate = (self.config_dir / path).resolve()
        if candidate.is_file():
            return candidate
        
        # Fall back to repository root relative path
        return (Path.cwd() / path).resolve()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run exemplar raw ingestion pipeline")
    default_config = Path(__file__).resolve().with_name("raw_pipeline_config.yaml")
    parser.add_argument(
        "--config",
        type=str,
        default=str(default_config),
        help="Path to pipeline configuration YAML",
    )
    parser.add_argument(
        "--connector",
        action="append",
        help="Connector name to run (repeatable)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Skip Kafka publishing step (extract + transform only)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging",
    )
    return parser.parse_args()


def _configure_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


async def _run_pipeline_async(args: argparse.Namespace) -> List[ConnectorRunSummary]:
    config_path = Path(args.config).resolve()
    pipeline = RawKafkaIngestionPipeline.from_path(config_path)
    return await pipeline.run(
        connector_filter=args.connector,
        dry_run=args.dry_run,
    )


def main() -> None:
    args = _parse_args()
    _configure_logging(args.verbose)
    
    try:
        summaries = asyncio.run(_run_pipeline_async(args))
    except KeyboardInterrupt:  # pragma: no cover - user cancellation
        logging.error("Pipeline interrupted by user")
        sys.exit(2)
    
    if not summaries:
        logging.warning("No connectors executed")
        sys.exit(0)
    
    for summary in summaries:
        print(json.dumps(asdict(summary), indent=2, default=str))
    
    if all(summary.status == "success" for summary in summaries):
        sys.exit(0)
    
    # Non-success statuses propagate non-zero exit
    sys.exit(1)


if __name__ == "__main__":  # pragma: no cover
    main()
