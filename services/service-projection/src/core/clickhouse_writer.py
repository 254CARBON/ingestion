"""
ClickHouse writer for projection service.

This module provides ClickHouse connectivity and batch writing capabilities
for the projection service.
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from contextlib import asynccontextmanager

import structlog
from clickhouse_driver import Client
from pydantic import BaseModel, Field


class ClickHouseWriterConfig(BaseModel):
    """Configuration for ClickHouse writer."""

    dsn: str = Field("clickhouse://default@localhost:9000/carbon_ingestion", description="ClickHouse DSN")
    pool_size: int = Field(10, description="Connection pool size")
    batch_size: int = Field(1000, description="Batch size for inserts")
    timeout: int = Field(30, description="Query timeout in seconds")
    retries: int = Field(3, description="Number of retries on failure")
    retry_delay: float = Field(1.0, description="Delay between retries in seconds")


class ClickHouseWriter:
    """
    ClickHouse writer for projection service.

    Handles batch writes to ClickHouse tables with connection pooling
    and retry logic.
    """

    def __init__(self, config: ClickHouseWriterConfig):
        """
        Initialize ClickHouse writer.

        Args:
            config: Writer configuration
        """
        self.config = config
        self.logger = structlog.get_logger(__name__)

        # Connection pool
        self._clients: List[Client] = []
        self._client_index = 0

        # Statistics
        self.stats = {
            "total_writes": 0,
            "successful_writes": 0,
            "failed_writes": 0,
            "rows_written": 0,
            "write_latency_ms": [],
            "connection_errors": 0,
            "query_errors": 0
        }

        # Initialize connection pool
        self._initialize_pool()

    def _initialize_pool(self) -> None:
        """Initialize connection pool."""
        try:
            for _ in range(self.config.pool_size):
                client = Client.from_url(self.config.dsn)
                self._clients.append(client)

            self.logger.info("ClickHouse connection pool initialized",
                           pool_size=self.config.pool_size,
                           dsn=self.config.dsn)

        except Exception as e:
            self.logger.error("Failed to initialize ClickHouse connection pool", error=str(e))
            raise

    def _get_client(self) -> Client:
        """Get next available client from pool."""
        if not self._clients:
            raise RuntimeError("No ClickHouse clients available")

        client = self._clients[self._client_index]
        self._client_index = (self._client_index + 1) % len(self._clients)
        return client

    async def write_ohlc_bars(self, ohlc_bars: List[Dict[str, Any]]) -> int:
        """
        Write OHLC bars to ClickHouse.

        Args:
            ohlc_bars: List of OHLC bar data

        Returns:
            int: Number of rows written
        """
        return await self._batch_write(
            "gold_ohlc_bars",
            ohlc_bars,
            [
                "market", "delivery_location", "delivery_date", "bar_type", "bar_size",
                "open_price", "high_price", "low_price", "close_price", "volume", "vwap",
                "trade_count", "price_currency", "quantity_unit", "aggregation_timestamp"
            ]
        )

    async def write_rolling_metrics(self, rolling_metrics: List[Dict[str, Any]]) -> int:
        """
        Write rolling metrics to ClickHouse.

        Args:
            rolling_metrics: List of rolling metrics data

        Returns:
            int: Number of rows written
        """
        return await self._batch_write(
            "gold_rolling_metrics",
            rolling_metrics,
            [
                "market", "delivery_location", "delivery_date", "metric_type", "window_size",
                "metric_value", "metric_unit", "sample_count", "calculation_timestamp"
            ]
        )

    async def write_curve_data(self, curve_data: List[Dict[str, Any]]) -> int:
        """
        Write curve data to ClickHouse.

        Args:
            curve_data: List of curve data

        Returns:
            int: Number of rows written
        """
        return await self._batch_write(
            "gold_curve_prestage",
            curve_data,
            [
                "market", "curve_type", "delivery_date", "delivery_hour", "price", "quantity",
                "price_currency", "quantity_unit", "curve_metadata", "aggregation_timestamp"
            ]
        )

    async def _batch_write(self, table: str, data: List[Dict[str, Any]], columns: List[str]) -> int:
        """
        Perform batch write to ClickHouse table.

        Args:
            table: Table name
            data: Data to write
            columns: Column names

        Returns:
            int: Number of rows written
        """
        if not data:
            return 0

        start_time = datetime.now(timezone.utc)

        try:
            self.stats["total_writes"] += 1

            # Prepare data for batch insert
            batch_data = []
            for row in data:
                batch_row = []
                for col in columns:
                    value = row.get(col)
                    # Handle None values
                    if value is None:
                        if col.endswith("_timestamp"):
                            value = datetime.now(timezone.utc)
                        elif col in ["price", "quantity", "volume", "vwap", "metric_value"]:
                            value = 0.0
                        elif col in ["trade_count", "sample_count", "delivery_hour"]:
                            value = 0
                        else:
                            value = ""
                    batch_row.append(value)
                batch_data.append(tuple(batch_row))

            # Execute batch insert with retry logic
            client = self._get_client()

            # Build INSERT query
            columns_str = ", ".join(columns)
            placeholders = ", ".join(["%s"] * len(columns))
            query = f"INSERT INTO carbon_ingestion.{table} ({columns_str}) VALUES ({placeholders})"

            # Execute with retries
            for attempt in range(self.config.retries):
                try:
                    client.execute(query, batch_data, types_check=True)
                    break
                except Exception as e:
                    if attempt == self.config.retries - 1:
                        raise e
                    self.logger.warning(f"Write attempt {attempt + 1} failed, retrying",
                                      table=table, error=str(e))
                    await asyncio.sleep(self.config.retry_delay * (2 ** attempt))  # Exponential backoff

            # Update statistics
            end_time = datetime.now(timezone.utc)
            latency_ms = (end_time - start_time).total_seconds() * 1000

            self.stats["successful_writes"] += 1
            self.stats["rows_written"] += len(data)
            self.stats["write_latency_ms"].append(latency_ms)

            # Keep only last 1000 latency measurements
            if len(self.stats["write_latency_ms"]) > 1000:
                self.stats["write_latency_ms"] = self.stats["write_latency_ms"][-1000:]

            self.logger.debug("Batch write completed",
                            table=table,
                            rows=len(data),
                            latency_ms=latency_ms)

            return len(data)

        except Exception as e:
            self.stats["failed_writes"] += 1
            self.logger.error("Batch write failed",
                            table=table,
                            rows=len(data),
                            error=str(e))
            raise

    async def query_ohlc_bars(
        self,
        market: Optional[str] = None,
        delivery_location: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        bar_type: str = "daily",
        limit: int = 1000
    ) -> List[Dict[str, Any]]:
        """
        Query OHLC bars from ClickHouse.

        Args:
            market: Market filter
            delivery_location: Location filter
            start_date: Start date filter (YYYY-MM-DD)
            end_date: End date filter (YYYY-MM-DD)
            bar_type: Bar type (daily, hourly)
            limit: Maximum number of results

        Returns:
            List[Dict[str, Any]]: Query results
        """
        try:
            client = self._get_client()

            # Build WHERE clause
            where_conditions = []
            if market:
                where_conditions.append(f"market = '{market}'")
            if delivery_location:
                where_conditions.append(f"delivery_location = '{delivery_location}'")
            if start_date:
                where_conditions.append(f"delivery_date >= '{start_date}'")
            if end_date:
                where_conditions.append(f"delivery_date <= '{end_date}'")
            where_conditions.append(f"bar_type = '{bar_type}'")

            where_clause = " AND ".join(where_conditions)
            query = f"""
                SELECT
                    market, delivery_location, delivery_date, bar_type, bar_size,
                    open_price, high_price, low_price, close_price, volume, vwap,
                    trade_count, price_currency, quantity_unit, aggregation_timestamp
                FROM carbon_ingestion.gold_ohlc_bars
                WHERE {where_clause}
                ORDER BY delivery_date DESC, delivery_location
                LIMIT {limit}
            """

            result = client.execute(query, with_column_types=True)

            # Convert to list of dicts
            if result:
                columns, rows = result
                column_names = [col[0] for col in columns]

                results = []
                for row in rows:
                    result_dict = dict(zip(column_names, row))
                    results.append(result_dict)

                return results

            return []

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
        limit: int = 1000
    ) -> List[Dict[str, Any]]:
        """
        Query rolling metrics from ClickHouse.

        Args:
            market: Market filter
            delivery_location: Location filter
            metric_type: Metric type filter
            start_date: Start date filter
            end_date: End date filter
            limit: Maximum number of results

        Returns:
            List[Dict[str, Any]]: Query results
        """
        try:
            client = self._get_client()

            # Build WHERE clause
            where_conditions = []
            if market:
                where_conditions.append(f"market = '{market}'")
            if delivery_location:
                where_conditions.append(f"delivery_location = '{delivery_location}'")
            if metric_type:
                where_conditions.append(f"metric_type = '{metric_type}'")
            if start_date:
                where_conditions.append(f"delivery_date >= '{start_date}'")
            if end_date:
                where_conditions.append(f"delivery_date <= '{end_date}'")

            where_clause = " AND ".join(where_conditions)
            query = f"""
                SELECT
                    market, delivery_location, delivery_date, metric_type, window_size,
                    metric_value, metric_unit, sample_count, calculation_timestamp
                FROM carbon_ingestion.gold_rolling_metrics
                WHERE {where_clause}
                ORDER BY delivery_date DESC, metric_type
                LIMIT {limit}
            """

            result = client.execute(query, with_column_types=True)

            # Convert to list of dicts
            if result:
                columns, rows = result
                column_names = [col[0] for col in columns]

                results = []
                for row in rows:
                    result_dict = dict(zip(column_names, row))
                    results.append(result_dict)

                return results

            return []

        except Exception as e:
            self.logger.error("Failed to query rolling metrics", error=str(e))
            return []

    async def refresh_materialized_view(self, view_name: str) -> bool:
        """
        Refresh a materialized view.

        Args:
            view_name: Name of the materialized view

        Returns:
            bool: True if successful
        """
        try:
            client = self._get_client()

            # For ClickHouse, materialized views are automatically refreshed
            # This method can be used for manual optimization if needed
            query = f"OPTIMIZE TABLE carbon_ingestion.{view_name}"

            client.execute(query)

            self.logger.info("Materialized view refreshed", view_name=view_name)
            return True

        except Exception as e:
            self.logger.error("Failed to refresh materialized view",
                            view_name=view_name, error=str(e))
            return False

    async def get_table_stats(self, table: str) -> Dict[str, Any]:
        """
        Get statistics for a ClickHouse table.

        Args:
            table: Table name

        Returns:
            Dict[str, Any]: Table statistics
        """
        try:
            client = self._get_client()

            query = f"""
                SELECT
                    name,
                    type,
                    formatReadableSize(sum(data_compressed_bytes)) as compressed_size,
                    formatReadableSize(sum(data_uncompressed_bytes)) as uncompressed_size,
                    sum(rows) as total_rows,
                    count() as parts_count
                FROM system.parts
                WHERE table = '{table}' AND database = 'carbon_ingestion'
                GROUP BY name, type
            """

            result = client.execute(query)

            if result:
                columns = ["name", "type", "compressed_size", "uncompressed_size", "total_rows", "parts_count"]
                stats = dict(zip(columns, result[0]))

                return stats

            return {}

        except Exception as e:
            self.logger.error("Failed to get table stats", table=table, error=str(e))
            return {}

    async def optimize_table(self, table: str) -> bool:
        """
        Optimize a ClickHouse table.

        Args:
            table: Table name

        Returns:
            bool: True if successful
        """
        try:
            client = self._get_client()

            query = f"OPTIMIZE TABLE carbon_ingestion.{table} FINAL"

            client.execute(query)

            self.logger.info("Table optimized", table=table)
            return True

        except Exception as e:
            self.logger.error("Failed to optimize table", table=table, error=str(e))
            return False

    def get_stats(self) -> Dict[str, Any]:
        """
        Get ClickHouse writer statistics.

        Returns:
            Dict[str, Any]: Writer statistics
        """
        avg_latency = (
            sum(self.stats["write_latency_ms"]) / len(self.stats["write_latency_ms"])
            if self.stats["write_latency_ms"] else 0
        )

        return {
            **self.stats,
            "average_write_latency_ms": avg_latency,
            "success_rate": (
                self.stats["successful_writes"] / max(1, self.stats["total_writes"])
            ),
            "pool_size": len(self._clients)
        }

    def reset_stats(self) -> None:
        """Reset writer statistics."""
        self.stats = {
            "total_writes": 0,
            "successful_writes": 0,
            "failed_writes": 0,
            "rows_written": 0,
            "write_latency_ms": [],
            "connection_errors": 0,
            "query_errors": 0
        }

    async def close(self) -> None:
        """Close all connections."""
        for client in self._clients:
            try:
                client.disconnect()
            except Exception as e:
                self.logger.warning("Error closing ClickHouse connection", error=str(e))

        self._clients.clear()
        self.logger.info("ClickHouse connections closed")
