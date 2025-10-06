#!/usr/bin/env python3
"""
CAISO Error Handling Example

This example demonstrates comprehensive error handling strategies for
CAISO connector operations including network failures, rate limiting,
data validation errors, and recovery mechanisms.
"""

import asyncio
import logging
import time
import sys
import os
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional

# Add project root to Python path for imports
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from connectors.caiso.connector import CAISOConnector
from connectors.caiso.config import CAISOConnectorConfig
from connectors.base import ExtractionResult
from connectors.base.exceptions import ExtractionError, TransformationError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CAISOErrorHandler:
    """Comprehensive error handling for CAISO operations."""

    def __init__(self, config: CAISOConnectorConfig):
        self.connector = CAISOConnector(config)
        self.error_stats = {
            "network_errors": 0,
            "rate_limit_errors": 0,
            "data_validation_errors": 0,
            "timeout_errors": 0,
            "node_not_found_errors": 0,
            "total_retries": 0,
            "successful_operations": 0
        }

    async def extract_with_retry(
        self,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
        **extract_kwargs
    ) -> Optional[ExtractionResult]:
        """
        Extract data with exponential backoff retry logic.

        Args:
            max_retries: Maximum number of retry attempts
            backoff_factor: Exponential backoff multiplier
            **extract_kwargs: Arguments for connector.extract()

        Returns:
            ExtractionResult or None if all retries failed
        """
        last_exception = None

        for attempt in range(max_retries + 1):
            try:
                self.error_stats["total_retries"] += 1

                result = await self.connector.extract(**extract_kwargs)

                self.error_stats["successful_operations"] += 1
                logger.info(f"Extraction successful on attempt {attempt + 1}")
                return result

            except ExtractionError as e:
                last_exception = e
                error_msg = str(e).lower()

                if "rate limit" in error_msg or "429" in error_msg:
                    self.error_stats["rate_limit_errors"] += 1
                    logger.warning(f"Rate limit hit on attempt {attempt + 1}")
                elif "timeout" in error_msg:
                    self.error_stats["timeout_errors"] += 1
                    logger.warning(f"Timeout on attempt {attempt + 1}")
                elif "node" in error_msg and "not found" in error_msg:
                    self.error_stats["node_not_found_errors"] += 1
                    logger.error(f"Node not found on attempt {attempt + 1}")
                    break  # Don't retry node not found errors
                else:
                    self.error_stats["network_errors"] += 1
                    logger.warning(f"Network error on attempt {attempt + 1}")

                if attempt < max_retries:
                    delay = backoff_factor ** attempt
                    logger.info(f"Retrying in {delay} seconds...")
                    await asyncio.sleep(delay)

            except Exception as e:
                last_exception = e
                self.error_stats["network_errors"] += 1
                logger.error(f"Unexpected error on attempt {attempt + 1}: {e}")

                if attempt < max_retries:
                    delay = backoff_factor ** attempt
                    logger.info(f"Retrying in {delay} seconds...")
                    await asyncio.sleep(delay)

        logger.error(f"All {max_retries + 1} attempts failed. Last error: {last_exception}")
        return None

    async def validate_and_transform_with_error_handling(
        self,
        extraction_result: ExtractionResult
    ) -> Optional[Dict[str, Any]]:
        """
        Validate and transform data with comprehensive error handling.

        Args:
            extraction_result: Raw extraction result

        Returns:
            Transformed data or None if transformation failed
        """
        try:
            # Check data quality before transformation
            validation_errors = []

            for record in extraction_result.data:
                # Basic validation
                if not record.get("event_id"):
                    validation_errors.append("Missing event_id")
                if not record.get("occurred_at"):
                    validation_errors.append("Missing occurred_at")
                if record.get("price") is None:
                    validation_errors.append("Missing price")

            if validation_errors:
                self.error_stats["data_validation_errors"] += len(validation_errors)
                logger.warning(f"Data validation errors: {validation_errors[:5]}...")  # Show first 5

            # Transform data
            transform_result = await self.connector.transform(extraction_result)

            logger.info(f"Transformation successful: {transform_result.record_count} records")
            return {
                "data": transform_result.data,
                "metadata": transform_result.metadata,
                "validation_errors": validation_errors
            }

        except TransformationError as e:
            logger.error(f"Transformation failed: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected transformation error: {e}")
            return None

    def get_error_summary(self) -> Dict[str, Any]:
        """Get comprehensive error statistics."""
        total_operations = self.error_stats["successful_operations"] + self.error_stats["total_retries"]
        success_rate = (
            self.error_stats["successful_operations"] / max(1, total_operations)
        ) * 100

        return {
            **self.error_stats,
            "total_operations": total_operations,
            "success_rate_percent": round(success_rate, 2),
            "error_rate_percent": round(100 - success_rate, 2)
        }


async def error_handling_example():
    """Comprehensive error handling example."""

    logger.info("=== CAISO Error Handling Example ===")

    # Create configuration with conservative settings for error-prone scenarios
    config = CAISOConnectorConfig(
        name="caiso_error_example",
        version="1.0.0",
        market="CAISO",
        mode="batch",
        output_topic="ingestion.caiso.raw.v1",
        schema="schemas/raw_caiso_trade.avsc",
        # Conservative settings for error scenarios
        caiso_timeout=45,
        caiso_rate_limit=50,  # Lower rate limit
        caiso_retry_attempts=3,
    )

    error_handler = CAISOErrorHandler(config)

    # Test scenarios that commonly cause errors

    # Scenario 1: Invalid node
    logger.info("Scenario 1: Invalid node handling")
    result = await error_handler.extract_with_retry(
        node="INVALID_NODE_12345",
        market_run_id="DAM",
        start="2024-01-01T00:00:00",
        end="2024-01-01T01:00:00"
    )

    if result is None:
        logger.info("✓ Invalid node correctly handled (no data returned)")
    else:
        logger.warning("⚠ Unexpected data from invalid node")

    # Scenario 2: Network timeout
    logger.info("Scenario 2: Network timeout simulation")
    # Note: In real scenarios, this would be tested with actual network conditions
    # Here we'll simulate by using a very short timeout

    # Scenario 3: Rate limiting
    logger.info("Scenario 3: Rate limiting handling")
    start_time = time.time()

    # Make multiple rapid requests to trigger rate limiting
    for i in range(5):
        result = await error_handler.extract_with_retry(
            node="TH_SP15_GEN-APND",
            market_run_id="DAM",
            start="2024-01-01T00:00:00",
            end="2024-01-01T01:00:00",
            max_retries=1  # Quick retries for testing
        )

        if i < 4:  # Don't wait after last request
            await asyncio.sleep(0.5)

    rate_limit_duration = time.time() - start_time
    logger.info(f"Rate limit test completed in {rate_limit_duration:.2f} seconds")

    # Scenario 4: Data validation errors
    logger.info("Scenario 4: Data validation error handling")

    # Create extraction result with invalid data
    invalid_data = [
        {
            "event_id": "",  # Invalid: empty event_id
            "occurred_at": None,  # Invalid: null timestamp
            "price": -100,  # Invalid: negative price
            "node": "TH_SP15_GEN-APND"
        }
    ]

    invalid_result = ExtractionResult(
        data=invalid_data,
        metadata={"test": "invalid_data"},
        record_count=1
    )

    transform_result = await error_handler.validate_and_transform_with_error_handling(invalid_result)

    if transform_result and transform_result["validation_errors"]:
        logger.info(f"✓ Data validation errors correctly detected: {len(transform_result['validation_errors'])} errors")

    # Scenario 5: Successful operation
    logger.info("Scenario 5: Successful operation")
    result = await error_handler.extract_with_retry(
        node="TH_SP15_GEN-APND",
        market_run_id="DAM",
        start="2024-01-01T00:00:00",
        end="2024-01-01T01:00:00"
    )

    if result and result.record_count > 0:
        logger.info(f"✓ Successful extraction: {result.record_count} records")

        # Test transformation
        transform_result = await error_handler.validate_and_transform_with_error_handling(result)
        if transform_result:
            logger.info(f"✓ Successful transformation: {len(transform_result['data'])} records")

    # Print comprehensive error summary
    summary = error_handler.get_error_summary()
    logger.info("=== Error Handling Summary ===")
    for key, value in summary.items():
        logger.info(f"{key}: {value}")

    return summary


async def circuit_breaker_example():
    """Example of circuit breaker pattern for CAISO operations."""

    logger.info("=== CAISO Circuit Breaker Example ===")

    class CAISOCircuitBreaker:
        """Simple circuit breaker implementation."""

        def __init__(self, failure_threshold: int = 5, recovery_timeout: int = 300):
            self.failure_threshold = failure_threshold
            self.recovery_timeout = recovery_timeout
            self.failure_count = 0
            self.last_failure_time = None
            self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN

        def can_execute(self) -> bool:
            """Check if operation can be executed."""
            if self.state == "CLOSED":
                return True
            elif self.state == "OPEN":
                if self.last_failure_time and \
                   (datetime.now(timezone.utc) - self.last_failure_time).total_seconds() > self.recovery_timeout:
                    self.state = "HALF_OPEN"
                    return True
                return False
            else:  # HALF_OPEN
                return True

        def record_success(self):
            """Record successful operation."""
            self.failure_count = 0
            self.state = "CLOSED"

        def record_failure(self):
            """Record failed operation."""
            self.failure_count += 1
            self.last_failure_time = datetime.now(timezone.utc)

            if self.failure_count >= self.failure_threshold:
                self.state = "OPEN"

    circuit_breaker = CAISOCircuitBreaker(failure_threshold=3, recovery_timeout=60)

    config = CAISOConnectorConfig(
        name="caiso_circuit_breaker",
        version="1.0.0",
        market="CAISO",
        mode="batch",
        output_topic="ingestion.caiso.raw.v1",
        schema="schemas/raw_caiso_trade.avsc"
    )

    connector = CAISOConnector(config)

    logger.info("Testing circuit breaker pattern...")

    # Simulate a series of failures followed by recovery
    test_scenarios = [
        ("INVALID_NODE_1", "should_fail"),
        ("INVALID_NODE_2", "should_fail"),
        ("INVALID_NODE_3", "should_fail"),
        ("TH_SP15_GEN-APND", "should_succeed"),
    ]

    for node, expected in test_scenarios:
        if not circuit_breaker.can_execute():
            logger.warning(f"Circuit breaker OPEN - skipping {node}")
            continue

        try:
            result = await connector.extract(
                market_run_id="DAM",
                node=node,
                start="2024-01-01T00:00:00",
                end="2024-01-01T01:00:00"
            )

            if result.record_count > 0:
                circuit_breaker.record_success()
                logger.info(f"✓ {node}: Success")
            else:
                circuit_breaker.record_failure()
                logger.warning(f"⚠ {node}: No data")

        except Exception as e:
            circuit_breaker.record_failure()
            logger.error(f"✗ {node}: Failed - {e}")

        # Brief delay between requests
        await asyncio.sleep(1)

    logger.info(f"Final circuit breaker state: {circuit_breaker.state}")
    logger.info(f"Failure count: {circuit_breaker.failure_count}")


async def graceful_degradation_example():
    """Example of graceful degradation when CAISO services are unavailable."""

    logger.info("=== CAISO Graceful Degradation Example ===")

    config = CAISOConnectorConfig(
        name="caiso_graceful",
        version="1.0.0",
        market="CAISO",
        mode="batch",
        output_topic="ingestion.caiso.raw.v1",
        schema="schemas/raw_caiso_trade.avsc"
    )

    connector = CAISOConnector(config)

    # Primary nodes (most important)
    primary_nodes = ["TH_SP15_GEN-APND"]

    # Fallback nodes (less critical but still valuable)
    fallback_nodes = ["TH_NP15_GEN-APND", "TH_ZP26_GEN-APND"]

    # Backup data sources (if CAISO is completely down)
    backup_sources = [
        "cached_data",
        "alternative_api",
        "historical_averages"
    ]

    async def try_extract_with_fallback(node_list: List[str], description: str) -> bool:
        """Try to extract from a list of nodes."""
        for node in node_list:
            try:
                result = await connector.extract(
                    market_run_id="DAM",
                    node=node,
                    start="2024-01-01T00:00:00",
                    end="2024-01-01T01:00:00"
                )

                if result.record_count > 0:
                    logger.info(f"✓ {description} - {node}: {result.record_count} records")
                    return True
                else:
                    logger.warning(f"⚠ {description} - {node}: No data")

            except Exception as e:
                logger.error(f"✗ {description} - {node}: {e}")

        return False

    logger.info("Attempting graceful degradation...")

    # Try primary nodes first
    success = await try_extract_with_fallback(primary_nodes, "Primary")

    if not success:
        logger.warning("Primary nodes failed, trying fallback nodes...")
        success = await try_extract_with_fallback(fallback_nodes, "Fallback")

        if not success:
            logger.error("All CAISO nodes failed, using backup data sources...")

            # Simulate using backup data
            for source in backup_sources:
                logger.info(f"Using backup source: {source}")
                # In real implementation, this would fetch from alternative sources
                # For this example, we'll just log it

            logger.info("✓ Backup data sources utilized")


async def alerting_example():
    """Example of alerting for CAISO operational issues."""

    logger.info("=== CAISO Alerting Example ===")

    class CAISOAlertManager:
        """Manages alerts for CAISO operational issues."""

        def __init__(self):
            self.alerts_sent = []
            self.alert_thresholds = {
                "error_rate": 0.1,  # Alert if >10% errors
                "data_gap_minutes": 30,  # Alert if data gap >30 min
                "price_anomaly": True,  # Alert on price anomalies
            }

        def check_and_alert(self, metrics: Dict[str, Any]):
            """Check metrics and send alerts if needed."""
            alerts = []

            # Check error rate
            error_rate = metrics.get("error_rate", 0)
            if error_rate > self.alert_thresholds["error_rate"]:
                alert = {
                    "type": "high_error_rate",
                    "severity": "warning",
                    "message": f"CAISO error rate is {error_rate*100:.1f}% (threshold: {self.alert_thresholds['error_rate']*100}%)",
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
                alerts.append(alert)

            # Check data gaps
            last_data_time = metrics.get("last_data_timestamp")
            if last_data_time:
                time_since_last_data = (datetime.now(timezone.utc) - last_data_time).total_seconds() / 60
                if time_since_last_data > self.alert_thresholds["data_gap_minutes"]:
                    alert = {
                        "type": "data_gap",
                        "severity": "critical",
                        "message": f"No CAISO data for {time_since_last_data:.0f} minutes",
                        "timestamp": datetime.now(timezone.utc).isoformat()
                    }
                    alerts.append(alert)

            # Send alerts
            for alert in alerts:
                self.alerts_sent.append(alert)
                logger.warning(f"ALERT [{alert['severity'].upper()}]: {alert['message']}")

                # In production, this would send to monitoring system
                # await send_alert_to_monitoring(alert)

            return alerts

    alert_manager = CAISOAlertManager()
    error_handler = CAISOErrorHandler(CAISOConnectorConfig(
        name="caiso_alerting",
        version="1.0.0",
        market="CAISO",
        mode="batch",
        output_topic="ingestion.caiso.raw.v1",
        schema="schemas/raw_caiso_trade.avsc"
    ))

    logger.info("Testing alerting system...")

    # Simulate some operations and check for alerts
    for i in range(5):
        result = await error_handler.extract_with_retry(
            node="TH_SP15_GEN-APND",
            market_run_id="DAM",
            start="2024-01-01T00:00:00",
            end="2024-01-01T01:00:00"
        )

        # Create mock metrics for alerting
        metrics = {
            "error_rate": 0.2 if i < 3 else 0.05,  # High error rate initially
            "last_data_timestamp": datetime.now(timezone.utc) - timedelta(minutes=45) if i == 2 else datetime.now(timezone.utc)
        }

        alerts = alert_manager.check_and_alert(metrics)

        if alerts:
            logger.info(f"Generated {len(alerts)} alerts")

        await asyncio.sleep(1)

    logger.info(f"Total alerts sent: {len(alert_manager.alerts_sent)}")


async def main():
    """Run all error handling examples."""

    try:
        # Comprehensive error handling
        await error_handling_example()
        print()

        # Circuit breaker pattern
        await circuit_breaker_example()
        print()

        # Graceful degradation
        await graceful_degradation_example()
        print()

        # Alerting system
        await alerting_example()

        logger.info("All error handling examples completed!")

    except Exception as e:
        logger.error(f"Error handling example failed: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
