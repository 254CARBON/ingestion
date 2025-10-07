"""
Core projection service implementation.

This module provides the main projection logic for writing aggregated market data
to ClickHouse for serving layer storage and Redis for caching.
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

from .clickhouse_writer import ClickHouseWriter, ClickHouseWriterConfig
from .cache_manager import CacheManager, CacheManagerConfig


class ProjectionResult(BaseModel):
    """Result of projection operation."""

    records_projected: int = Field(..., description="Number of records projected")
    tables_written: List[str] = Field(default_factory=list, description="Tables written to")
    cache_entries: int = Field(default=0, description="Cache entries created")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Projection metadata")
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class ProjectionService:
    """
    Core projection service for market data.

    This service handles the projection of aggregated market data to the
    serving layer using ClickHouse for storage and Redis for caching.
    """

    def __init__(self, config_path: str = "configs/projection_policies.yaml"):
        """
        Initialize the projection service.

        Args:
            config_path: Path to projection policies configuration
        """
        self.config_path = config_path
        self.logger = structlog.get_logger(__name__)

        # Load configuration
        self.config = self._load_config()

        # Initialize components
        self.clickhouse_writer = ClickHouseWriter(
            ClickHouseWriterConfig(
                dsn=self.config.get("clickhouse", {}).get("dsn", "clickhouse://default@localhost:9000/carbon_ingestion"),
                pool_size=self.config.get("clickhouse", {}).get("pool_size", 10),
                batch_size=self.config.get("clickhouse", {}).get("batch_size", 1000)
            )
        )

        self.cache_manager = CacheManager(
            CacheManagerConfig(
                redis_url=self.config.get("redis", {}).get("url", "redis://localhost:6379/0"),
                default_ttl=self.config.get("cache", {}).get("default_ttl", 3600),
                max_memory=self.config.get("cache", {}).get("max_memory", "1GB")
            )
        )

        # Projection statistics
        self.stats = {
            "total_projections": 0,
            "successful_projections": 0,
            "failed_projections": 0,
            "records_projected": 0,
            "cache_entries_created": 0,
            "clickhouse_writes": 0,
            "projection_times": []
        }

        self.logger.info("Projection service initialized",
                        clickhouse_dsn=self.clickhouse_writer.config.dsn,
                        redis_url=self.cache_manager.config.redis_url)

    def _load_config(self) -> Dict[str, Any]:
        """
        Load projection configuration.

        Returns:
            Dict[str, Any]: Configuration data
        """
        try:
            with open(self.config_path, 'r') as f:
                config = yaml.safe_load(f)
            self.logger.info("Projection configuration loaded", config_path=self.config_path)
            return config
        except Exception as e:
            self.logger.error("Failed to load projection configuration",
                            error=str(e), config_path=self.config_path)
            return {}

    async def project_ohlc_bars(self, ohlc_bars: List[Dict[str, Any]]) -> ProjectionResult:
        """
        Project OHLC bars to ClickHouse.

        Args:
            ohlc_bars: List of OHLC bar data

        Returns:
            ProjectionResult: Projection result
        """
        try:
            self.stats["total_projections"] += 1

            if not ohlc_bars:
                return ProjectionResult(
                    records_projected=0,
                    tables_written=[],
                    metadata={"reason": "empty_data"}
                )

            # Prepare data for ClickHouse
            clickhouse_data = []
            for bar in ohlc_bars:
                clickhouse_data.append({
                    "market": bar.get("market"),
                    "delivery_location": bar.get("delivery_location"),
                    "delivery_date": bar.get("delivery_date"),
                    "bar_type": bar.get("bar_type", "daily"),
                    "bar_size": bar.get("bar_size", "1D"),
                    "open_price": float(bar.get("open_price", 0)),
                    "high_price": float(bar.get("high_price", 0)),
                    "low_price": float(bar.get("low_price", 0)),
                    "close_price": float(bar.get("close_price", 0)),
                    "volume": float(bar.get("volume", 0)),
                    "vwap": float(bar.get("vwap", 0)),
                    "trade_count": int(bar.get("trade_count", 0)),
                    "price_currency": bar.get("price_currency", "USD"),
                    "quantity_unit": bar.get("quantity_unit", "MWh"),
                    "aggregation_timestamp": datetime.now(timezone.utc)
                })

            # Write to ClickHouse
            written_count = await self.clickhouse_writer.write_ohlc_bars(clickhouse_data)
            self.stats["clickhouse_writes"] += 1
            self.stats["records_projected"] += written_count

            # Cache latest bars for each market/location
            cache_entries = 0
            for bar in ohlc_bars:
                cache_key = f"ohlc:{bar.get('market')}:{bar.get('delivery_location')}:{bar.get('bar_type')}"
                await self.cache_manager.set(cache_key, bar, ttl=3600)  # 1 hour cache
                cache_entries += 1

            self.stats["cache_entries_created"] += cache_entries
            self.stats["successful_projections"] += 1

            result = ProjectionResult(
                records_projected=written_count,
                tables_written=["gold_ohlc_bars"],
                cache_entries=cache_entries,
                metadata={
                    "projection_type": "ohlc_bars",
                    "batch_size": len(ohlc_bars),
                    "markets": list(set(bar.get("market") for bar in ohlc_bars))
                }
            )

            self.logger.info("OHLC bars projected successfully",
                           records=written_count,
                           cache_entries=cache_entries)

            return result

        except Exception as e:
            self.stats["failed_projections"] += 1
            self.logger.error("Failed to project OHLC bars", error=str(e))

            return ProjectionResult(
                records_projected=0,
                metadata={"error": str(e)}
            )

    async def project_rolling_metrics(self, rolling_metrics: List[Dict[str, Any]]) -> ProjectionResult:
        """
        Project rolling metrics to ClickHouse.

        Args:
            rolling_metrics: List of rolling metrics data

        Returns:
            ProjectionResult: Projection result
        """
        try:
            self.stats["total_projections"] += 1

            if not rolling_metrics:
                return ProjectionResult(
                    records_projected=0,
                    tables_written=[],
                    metadata={"reason": "empty_data"}
                )

            # Prepare data for ClickHouse
            clickhouse_data = []
            for metric in rolling_metrics:
                clickhouse_data.append({
                    "market": metric.get("market"),
                    "delivery_location": metric.get("delivery_location"),
                    "delivery_date": metric.get("delivery_date"),
                    "metric_type": metric.get("metric_type"),
                    "window_size": metric.get("window_size"),
                    "metric_value": float(metric.get("metric_value", 0)),
                    "metric_unit": metric.get("metric_unit", "USD/MWh"),
                    "sample_count": int(metric.get("sample_count", 0)),
                    "calculation_timestamp": datetime.now(timezone.utc)
                })

            # Write to ClickHouse
            written_count = await self.clickhouse_writer.write_rolling_metrics(clickhouse_data)
            self.stats["clickhouse_writes"] += 1
            self.stats["records_projected"] += written_count

            # Cache latest metrics for each market/location/metric_type
            cache_entries = 0
            for metric in rolling_metrics:
                cache_key = f"metric:{metric.get('market')}:{metric.get('delivery_location')}:{metric.get('metric_type')}"
                await self.cache_manager.set(cache_key, metric, ttl=1800)  # 30 minute cache
                cache_entries += 1

            self.stats["cache_entries_created"] += cache_entries
            self.stats["successful_projections"] += 1

            result = ProjectionResult(
                records_projected=written_count,
                tables_written=["gold_rolling_metrics"],
                cache_entries=cache_entries,
                metadata={
                    "projection_type": "rolling_metrics",
                    "batch_size": len(rolling_metrics),
                    "metric_types": list(set(metric.get("metric_type") for metric in rolling_metrics))
                }
            )

            self.logger.info("Rolling metrics projected successfully",
                           records=written_count,
                           cache_entries=cache_entries)

            return result

        except Exception as e:
            self.stats["failed_projections"] += 1
            self.logger.error("Failed to project rolling metrics", error=str(e))

            return ProjectionResult(
                records_projected=0,
                metadata={"error": str(e)}
            )

    async def project_curve_data(self, curve_data: List[Dict[str, Any]]) -> ProjectionResult:
        """
        Project curve data to ClickHouse.

        Args:
            curve_data: List of curve data

        Returns:
            ProjectionResult: Projection result
        """
        try:
            self.stats["total_projections"] += 1

            if not curve_data:
                return ProjectionResult(
                    records_projected=0,
                    tables_written=[],
                    metadata={"reason": "empty_data"}
                )

            # Prepare data for ClickHouse
            clickhouse_data = []
            for curve in curve_data:
                clickhouse_data.append({
                    "market": curve.get("market"),
                    "curve_type": curve.get("curve_type"),
                    "delivery_date": curve.get("delivery_date"),
                    "delivery_hour": int(curve.get("delivery_hour", 0)),
                    "price": float(curve.get("price", 0)),
                    "quantity": float(curve.get("quantity", 0)),
                    "price_currency": curve.get("price_currency", "USD"),
                    "quantity_unit": curve.get("quantity_unit", "MWh"),
                    "curve_metadata": json.dumps(curve.get("curve_metadata", {})),
                    "aggregation_timestamp": datetime.now(timezone.utc)
                })

            # Write to ClickHouse
            written_count = await self.clickhouse_writer.write_curve_data(clickhouse_data)
            self.stats["clickhouse_writes"] += 1
            self.stats["records_projected"] += written_count

            # Cache latest curve points for each market/curve_type
            cache_entries = 0
            for curve in curve_data:
                cache_key = f"curve:{curve.get('market')}:{curve.get('curve_type')}"
                await self.cache_manager.set(cache_key, curve, ttl=7200)  # 2 hour cache
                cache_entries += 1

            self.stats["cache_entries_created"] += cache_entries
            self.stats["successful_projections"] += 1

            result = ProjectionResult(
                records_projected=written_count,
                tables_written=["gold_curve_prestage"],
                cache_entries=cache_entries,
                metadata={
                    "projection_type": "curve_data",
                    "batch_size": len(curve_data),
                    "curve_types": list(set(curve.get("curve_type") for curve in curve_data))
                }
            )

            self.logger.info("Curve data projected successfully",
                           records=written_count,
                           cache_entries=cache_entries)

            return result

        except Exception as e:
            self.stats["failed_projections"] += 1
            self.logger.error("Failed to project curve data", error=str(e))

            return ProjectionResult(
                records_projected=0,
                metadata={"error": str(e)}
            )

    async def manage_materialized_views(self) -> Dict[str, Any]:
        """
        Manage materialized views for the serving layer.

        Returns:
            Dict[str, Any]: Management result
        """
        try:
            result = {
                "refreshed_views": [],
                "errors": [],
                "timestamp": datetime.now(timezone.utc).isoformat()
            }

            # Refresh daily OHLC materialized view
            try:
                await self.clickhouse_writer.refresh_materialized_view("mv_daily_ohlc_bars")
                result["refreshed_views"].append("mv_daily_ohlc_bars")
            except Exception as e:
                result["errors"].append(f"Failed to refresh mv_daily_ohlc_bars: {str(e)}")

            # Refresh hourly OHLC materialized view
            try:
                await self.clickhouse_writer.refresh_materialized_view("mv_hourly_ohlc_bars")
                result["refreshed_views"].append("mv_hourly_ohlc_bars")
            except Exception as e:
                result["errors"].append(f"Failed to refresh mv_hourly_ohlc_bars: {str(e)}")

            self.logger.info("Materialized view management completed",
                           refreshed=len(result["refreshed_views"]),
                           errors=len(result["errors"]))

            return result

        except Exception as e:
            self.logger.error("Failed to manage materialized views", error=str(e))
            return {
                "refreshed_views": [],
                "errors": [str(e)],
                "timestamp": datetime.now(timezone.utc).isoformat()
            }

    async def query_ohlc_bars(
        self,
        market: Optional[str] = None,
        delivery_location: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        bar_type: str = "daily",
        use_cache: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Query OHLC bars with optional caching.

        Args:
            market: Market filter
            delivery_location: Location filter
            start_date: Start date filter (YYYY-MM-DD)
            end_date: End date filter (YYYY-MM-DD)
            bar_type: Bar type (daily, hourly)
            use_cache: Whether to use cache

        Returns:
            List[Dict[str, Any]]: OHLC bars data
        """
        try:
            # Try cache first if enabled
            if use_cache:
                cache_key = f"query:ohlc:{market or 'all'}:{delivery_location or 'all'}:{bar_type}:{start_date or 'none'}:{end_date or 'none'}"
                cached_result = await self.cache_manager.get(cache_key)
                if cached_result:
                    self.logger.debug("OHLC query served from cache", cache_key=cache_key)
                    return cached_result

            # Query ClickHouse
            result = await self.clickhouse_writer.query_ohlc_bars(
                market=market,
                delivery_location=delivery_location,
                start_date=start_date,
                end_date=end_date,
                bar_type=bar_type
            )

            # Cache the result
            if use_cache and result:
                await self.cache_manager.set(cache_key, result, ttl=300)  # 5 minute cache

            self.logger.debug("OHLC query executed",
                            records=len(result),
                            cached=use_cache and cached_result is not None)

            return result

        except Exception as e:
            self.logger.error("Failed to query OHLC bars", error=str(e))
            return []

    async def query_rolling_metrics(
        self,
        market: Optional[str] = None,
        delivery_location: Optional[str] = None,
        metric_type: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        use_cache: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Query rolling metrics with optional caching.

        Args:
            market: Market filter
            delivery_location: Location filter
            metric_type: Metric type filter
            start_date: Start date filter
            end_date: End date filter
            use_cache: Whether to use cache

        Returns:
            List[Dict[str, Any]]: Rolling metrics data
        """
        try:
            # Try cache first if enabled
            if use_cache:
                cache_key = f"query:metrics:{market or 'all'}:{delivery_location or 'all'}:{metric_type or 'all'}:{start_date or 'none'}:{end_date or 'none'}"
                cached_result = await self.cache_manager.get(cache_key)
                if cached_result:
                    self.logger.debug("Metrics query served from cache", cache_key=cache_key)
                    return cached_result

            # Query ClickHouse
            result = await self.clickhouse_writer.query_rolling_metrics(
                market=market,
                delivery_location=delivery_location,
                metric_type=metric_type,
                start_date=start_date,
                end_date=end_date
            )

            # Cache the result
            if use_cache and result:
                await self.cache_manager.set(cache_key, result, ttl=300)  # 5 minute cache

            self.logger.debug("Metrics query executed",
                            records=len(result),
                            cached=use_cache and cached_result is not None)

            return result

        except Exception as e:
            self.logger.error("Failed to query rolling metrics", error=str(e))
            return []

    def get_stats(self) -> Dict[str, Any]:
        """
        Get projection service statistics.

        Returns:
            Dict[str, Any]: Service statistics
        """
        # Calculate averages
        avg_projection_time = (
            sum(self.stats["projection_times"]) / len(self.stats["projection_times"])
            if self.stats["projection_times"] else 0
        )

        return {
            **self.stats,
            "average_projection_time_ms": avg_projection_time * 1000,
            "success_rate": (
                self.stats["successful_projections"] / max(1, self.stats["total_projections"])
            ),
            "clickhouse_writer_stats": self.clickhouse_writer.get_stats(),
            "cache_manager_stats": self.cache_manager.get_stats()
        }

    def reset_stats(self) -> None:
        """Reset projection service statistics."""
        self.stats = {
            "total_projections": 0,
            "successful_projections": 0,
            "failed_projections": 0,
            "records_projected": 0,
            "cache_entries_created": 0,
            "clickhouse_writes": 0,
            "projection_times": []
        }
