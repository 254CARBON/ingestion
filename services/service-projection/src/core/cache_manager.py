"""
Redis cache manager for projection service.

This module provides Redis caching capabilities for the projection service,
including connection pooling, TTL management, and cache invalidation.
"""

import asyncio
import json
import logging
from typing import Any, Dict, List, Optional, Set
from datetime import datetime, timezone

import structlog
import redis.asyncio as redis
from pydantic import BaseModel, Field


class CacheManagerConfig(BaseModel):
    """Configuration for Redis cache manager."""

    redis_url: str = Field("redis://localhost:6379/0", description="Redis URL")
    pool_size: int = Field(10, description="Connection pool size")
    default_ttl: int = Field(3600, description="Default TTL in seconds")
    max_memory: str = Field("1GB", description="Max memory usage")
    retry_on_failure: bool = Field(True, description="Retry on Redis failures")
    max_retries: int = Field(3, description="Maximum retry attempts")
    retry_delay: float = Field(1.0, description="Delay between retries")


class CacheManager:
    """
    Redis cache manager for projection service.

    Provides caching capabilities for frequently accessed data with TTL
    management and cache invalidation strategies.
    """

    def __init__(self, config: CacheManagerConfig):
        """
        Initialize cache manager.

        Args:
            config: Cache manager configuration
        """
        self.config = config
        self.logger = structlog.get_logger(__name__)

        # Redis connection pool
        self._pool: Optional[redis.ConnectionPool] = None
        self._redis: Optional[redis.Redis] = None

        # Cache statistics
        self.stats = {
            "total_operations": 0,
            "successful_operations": 0,
            "failed_operations": 0,
            "cache_hits": 0,
            "cache_misses": 0,
            "cache_sets": 0,
            "cache_gets": 0,
            "cache_deletes": 0,
            "cache_invalidations": 0,
            "operation_latency_ms": []
        }

        # Initialize Redis connection
        self._initialize_connection()

    def _initialize_connection(self) -> None:
        """Initialize Redis connection pool."""
        try:
            self._pool = redis.ConnectionPool.from_url(
                self.config.redis_url,
                max_connections=self.config.pool_size,
                decode_responses=True
            )
            self._redis = redis.Redis(connection_pool=self._pool)

            # Test connection
            asyncio.create_task(self._test_connection())

            self.logger.info("Redis cache manager initialized",
                           redis_url=self.config.redis_url,
                           pool_size=self.config.pool_size)

        except Exception as e:
            self.logger.error("Failed to initialize Redis connection", error=str(e))
            raise

    async def _test_connection(self) -> bool:
        """Test Redis connection."""
        try:
            if self._redis:
                await self._redis.ping()
                self.logger.debug("Redis connection test successful")
                return True
        except Exception as e:
            self.logger.error("Redis connection test failed", error=str(e))

        return False

    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """
        Set a value in cache.

        Args:
            key: Cache key
            value: Value to cache
            ttl: Time to live in seconds (uses default if None)

        Returns:
            bool: True if successful
        """
        start_time = datetime.now(timezone.utc)

        try:
            self.stats["total_operations"] += 1
            self.stats["cache_sets"] += 1

            if not self._redis:
                raise RuntimeError("Redis not initialized")

            # Serialize value
            serialized_value = json.dumps(value, default=str)

            # Set with TTL
            ttl = ttl or self.config.default_ttl
            await self._redis.setex(key, ttl, serialized_value)

            # Update statistics
            end_time = datetime.now(timezone.utc)
            latency_ms = (end_time - start_time).total_seconds() * 1000

            self.stats["successful_operations"] += 1
            self.stats["operation_latency_ms"].append(latency_ms)

            # Keep only last 1000 latency measurements
            if len(self.stats["operation_latency_ms"]) > 1000:
                self.stats["operation_latency_ms"] = self.stats["operation_latency_ms"][-1000:]

            self.logger.debug("Cache set successful", key=key, ttl=ttl)

            return True

        except Exception as e:
            self.stats["failed_operations"] += 1
            self.logger.error("Failed to set cache", key=key, error=str(e))
            return False

    async def get(self, key: str) -> Optional[Any]:
        """
        Get a value from cache.

        Args:
            key: Cache key

        Returns:
            Optional[Any]: Cached value or None if not found
        """
        start_time = datetime.now(timezone.utc)

        try:
            self.stats["total_operations"] += 1
            self.stats["cache_gets"] += 1

            if not self._redis:
                raise RuntimeError("Redis not initialized")

            # Get from Redis
            value = await self._redis.get(key)

            if value is None:
                self.stats["cache_misses"] += 1
                return None

            # Deserialize value
            try:
                deserialized_value = json.loads(value)
                self.stats["cache_hits"] += 1
                self.stats["successful_operations"] += 1

                # Update latency statistics
                end_time = datetime.now(timezone.utc)
                latency_ms = (end_time - start_time).total_seconds() * 1000
                self.stats["operation_latency_ms"].append(latency_ms)

                if len(self.stats["operation_latency_ms"]) > 1000:
                    self.stats["operation_latency_ms"] = self.stats["operation_latency_ms"][-1000:]

                return deserialized_value

            except json.JSONDecodeError as e:
                self.logger.warning("Failed to deserialize cached value", key=key, error=str(e))
                # Delete corrupted cache entry
                await self.delete(key)
                return None

        except Exception as e:
            self.stats["failed_operations"] += 1
            self.logger.error("Failed to get cache", key=key, error=str(e))
            return None

    async def delete(self, key: str) -> bool:
        """
        Delete a value from cache.

        Args:
            key: Cache key

        Returns:
            bool: True if successful
        """
        try:
            self.stats["total_operations"] += 1
            self.stats["cache_deletes"] += 1

            if not self._redis:
                raise RuntimeError("Redis not initialized")

            result = await self._redis.delete(key)

            if result > 0:
                self.stats["successful_operations"] += 1
                self.logger.debug("Cache delete successful", key=key)
                return True
            else:
                self.logger.debug("Cache key not found for deletion", key=key)
                return False

        except Exception as e:
            self.stats["failed_operations"] += 1
            self.logger.error("Failed to delete cache", key=key, error=str(e))
            return False

    async def exists(self, key: str) -> bool:
        """
        Check if a key exists in cache.

        Args:
            key: Cache key

        Returns:
            bool: True if key exists
        """
        try:
            if not self._redis:
                raise RuntimeError("Redis not initialized")

            return bool(await self._redis.exists(key))

        except Exception as e:
            self.logger.error("Failed to check cache existence", key=key, error=str(e))
            return False

    async def expire(self, key: str, ttl: int) -> bool:
        """
        Set TTL for a cache key.

        Args:
            key: Cache key
            ttl: Time to live in seconds

        Returns:
            bool: True if successful
        """
        try:
            if not self._redis:
                raise RuntimeError("Redis not initialized")

            return bool(await self._redis.expire(key, ttl))

        except Exception as e:
            self.logger.error("Failed to set cache TTL", key=key, error=str(e))
            return False

    async def invalidate_pattern(self, pattern: str) -> int:
        """
        Invalidate cache entries matching a pattern.

        Args:
            pattern: Pattern to match (e.g., "query:ohlc:*")

        Returns:
            int: Number of keys invalidated
        """
        try:
            self.stats["total_operations"] += 1
            self.stats["cache_invalidations"] += 1

            if not self._redis:
                raise RuntimeError("Redis not initialized")

            # Get all keys matching pattern
            keys = await self._redis.keys(pattern)

            if not keys:
                return 0

            # Delete all matching keys
            deleted = await self._redis.delete(*keys)

            self.stats["successful_operations"] += 1
            self.logger.info("Cache pattern invalidation completed",
                           pattern=pattern,
                           keys_deleted=deleted)

            return deleted

        except Exception as e:
            self.stats["failed_operations"] += 1
            self.logger.error("Failed to invalidate cache pattern",
                           pattern=pattern, error=str(e))
            return 0

    async def invalidate_by_market(self, market: str) -> int:
        """
        Invalidate all cache entries for a specific market.

        Args:
            market: Market identifier

        Returns:
            int: Number of keys invalidated
        """
        patterns = [
            f"ohlc:{market}:*",
            f"metric:{market}:*",
            f"curve:{market}:*",
            f"query:ohlc:{market}:*",
            f"query:metrics:{market}:*"
        ]

        total_invalidated = 0
        for pattern in patterns:
            invalidated = await self.invalidate_pattern(pattern)
            total_invalidated += invalidated

        self.logger.info("Market cache invalidation completed",
                        market=market,
                        total_invalidated=total_invalidated)

        return total_invalidated

    async def invalidate_by_location(self, location: str) -> int:
        """
        Invalidate all cache entries for a specific location.

        Args:
            location: Location identifier

        Returns:
            int: Number of keys invalidated
        """
        patterns = [
            f"*:*:{location}:*",
            f"query:*:*:{location}:*"
        ]

        total_invalidated = 0
        for pattern in patterns:
            invalidated = await self.invalidate_pattern(pattern)
            total_invalidated += invalidated

        self.logger.info("Location cache invalidation completed",
                        location=location,
                        total_invalidated=total_invalidated)

        return total_invalidated

    async def get_cache_info(self) -> Dict[str, Any]:
        """
        Get Redis cache information.

        Returns:
            Dict[str, Any]: Cache information
        """
        try:
            if not self._redis:
                raise RuntimeError("Redis not initialized")

            info = await self._redis.info()

            return {
                "connected_clients": info.get("connected_clients", 0),
                "used_memory_human": info.get("used_memory_human", "0B"),
                "keyspace_hits": info.get("keyspace_hits", 0),
                "keyspace_misses": info.get("keyspace_misses", 0),
                "evicted_keys": info.get("evicted_keys", 0),
                "expired_keys": info.get("expired_keys", 0),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }

        except Exception as e:
            self.logger.error("Failed to get cache info", error=str(e))
            return {"error": str(e)}

    async def clear_all(self) -> bool:
        """
        Clear all cache entries.

        Returns:
            bool: True if successful
        """
        try:
            if not self._redis:
                raise RuntimeError("Redis not initialized")

            await self._redis.flushdb()

            self.logger.info("All cache cleared")
            return True

        except Exception as e:
            self.logger.error("Failed to clear cache", error=str(e))
            return False

    async def prefetch_hot_data(self, market: str, data_types: List[str]) -> int:
        """
        Prefetch hot data for a market.

        Args:
            market: Market identifier
            data_types: List of data types to prefetch

        Returns:
            int: Number of entries prefetched
        """
        try:
            prefetched = 0

            for data_type in data_types:
                if data_type == "ohlc":
                    # Prefetch latest OHLC bars for common locations
                    common_locations = ["HUB", "TH_SP15_GEN-APND", "HUB.MISO"]
                    for location in common_locations:
                        cache_key = f"ohlc:{market}:{location}:daily"
                        # This would typically query the database and cache the result
                        # For now, just log the intent
                        self.logger.debug("Would prefetch OHLC data",
                                        market=market,
                                        location=location,
                                        cache_key=cache_key)

                elif data_type == "metrics":
                    # Prefetch latest metrics
                    common_metrics = ["rolling_average_price", "rolling_total_volume"]
                    for metric in common_metrics:
                        cache_key = f"metric:{market}:*:rolling_average_price"
                        self.logger.debug("Would prefetch metrics data",
                                        market=market,
                                        metric=metric,
                                        cache_key=cache_key)

            self.logger.info("Hot data prefetch completed",
                           market=market,
                           data_types=data_types)

            return prefetched

        except Exception as e:
            self.logger.error("Failed to prefetch hot data",
                            market=market, error=str(e))
            return 0

    def get_stats(self) -> Dict[str, Any]:
        """
        Get cache manager statistics.

        Returns:
            Dict[str, Any]: Cache statistics
        """
        avg_latency = (
            sum(self.stats["operation_latency_ms"]) / len(self.stats["operation_latency_ms"])
            if self.stats["operation_latency_ms"] else 0
        )

        hit_rate = (
            self.stats["cache_hits"] / max(1, self.stats["cache_hits"] + self.stats["cache_misses"])
        )

        return {
            **self.stats,
            "average_operation_latency_ms": avg_latency,
            "cache_hit_rate": hit_rate,
            "success_rate": (
                self.stats["successful_operations"] / max(1, self.stats["total_operations"])
            )
        }

    def reset_stats(self) -> None:
        """Reset cache manager statistics."""
        self.stats = {
            "total_operations": 0,
            "successful_operations": 0,
            "failed_operations": 0,
            "cache_hits": 0,
            "cache_misses": 0,
            "cache_sets": 0,
            "cache_gets": 0,
            "cache_deletes": 0,
            "cache_invalidations": 0,
            "operation_latency_ms": []
        }

    async def close(self) -> None:
        """Close Redis connection."""
        if self._redis:
            await self._redis.close()
            self._redis = None

        if self._pool:
            await self._pool.disconnect()
            self._pool = None

        self.logger.info("Redis cache manager closed")
