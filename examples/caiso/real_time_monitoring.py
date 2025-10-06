#!/usr/bin/env python3
"""
CAISO Real-Time Monitoring Example

This example demonstrates continuous real-time monitoring of CAISO market data
for live dashboards, alerting, and immediate processing.
"""

import asyncio
import logging
import signal
import json
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

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CAISORealTimeMonitor:
    """Real-time monitor for CAISO market data."""

    def __init__(self, config: CAISOConnectorConfig):
        self.connector = CAISOConnector(config)
        self.monitoring_interval = 60  # seconds
        self.is_running = False
        self.stats = {
            "total_extractions": 0,
            "successful_extractions": 0,
            "failed_extractions": 0,
            "total_records": 0,
            "start_time": None
        }

    async def start_monitoring(self, nodes: List[str], market_run_id: str = "RTM"):
        """Start continuous real-time monitoring."""
        self.is_running = True
        self.stats["start_time"] = datetime.now(timezone.utc)

        logger.info(f"Starting real-time monitoring for nodes: {nodes}")
        logger.info(f"Market run: {market_run_id}")
        logger.info(f"Interval: {self.monitoring_interval} seconds")

        try:
            while self.is_running:
                await self._monitoring_cycle(nodes, market_run_id)
                await asyncio.sleep(self.monitoring_interval)

        except KeyboardInterrupt:
            logger.info("Monitoring stopped by user")
        except Exception as e:
            logger.error(f"Monitoring failed: {e}")
        finally:
            self._print_final_stats()

    def stop_monitoring(self):
        """Stop the monitoring process."""
        self.is_running = False

    async def _monitoring_cycle(self, nodes: List[str], market_run_id: str):
        """Execute one monitoring cycle."""
        cycle_start = datetime.now(timezone.utc)
        self.stats["total_extractions"] += 1

        logger.info(f"Monitoring cycle {self.stats['total_extractions']} started")

        for node in nodes:
            try:
                # Extract latest data for this node
                result = await self.connector.extract(
                    market_run_id=market_run_id,
                    node=node
                    # Uses current time window automatically
                )

                self.stats["total_records"] += result.record_count

                if result.record_count > 0:
                    self.stats["successful_extractions"] += 1

                    # Process the latest data point
                    latest_record = max(result.data, key=lambda x: x.get('occurred_at', 0))

                    await self._process_real_time_data(node, latest_record)

                    logger.info(f"Node {node}: {result.record_count} records, latest price: ${latest_record.get('price', 'N/A')}")
                else:
                    logger.warning(f"Node {node}: No data received")

            except Exception as e:
                self.stats["failed_extractions"] += 1
                logger.error(f"Failed to extract data for node {node}: {e}")

        cycle_duration = (datetime.now(timezone.utc) - cycle_start).total_seconds()
        logger.info(f"Cycle completed in {cycle_duration:.2f}s")

    async def _process_real_time_data(self, node: str, record: Dict[str, Any]):
        """Process real-time data (e.g., send to dashboard, check alerts)."""
        # Example: Check for price spikes
        price = record.get('price')
        if price and price > 100:  # Arbitrary threshold
            logger.warning(f"Price spike detected for node {node}: ${price}")

        # Example: Send to real-time dashboard (mock)
        # await send_to_dashboard(node, record)

        # Example: Check data quality
        if not record.get('event_id') or not record.get('occurred_at'):
            logger.warning(f"Incomplete data for node {node}")

    def _print_final_stats(self):
        """Print final monitoring statistics."""
        if not self.stats["start_time"]:
            return

        duration = datetime.now(timezone.utc) - self.stats["start_time"]
        duration_minutes = duration.total_seconds() / 60

        logger.info("=== Monitoring Session Summary ===")
        logger.info(f"Duration: {duration_minutes:.1f} minutes")
        logger.info(f"Total cycles: {self.stats['total_extractions']}")
        logger.info(f"Successful cycles: {self.stats['successful_extractions']}")
        logger.info(f"Failed cycles: {self.stats['failed_extractions']}")
        logger.info(f"Success rate: {(self.stats['successful_extractions']/max(1, self.stats['total_extractions']))*100:.1f}%")
        logger.info(f"Total records processed: {self.stats['total_records']}")
        logger.info(f"Average records per cycle: {self.stats['total_records']/max(1, self.stats['total_extractions']):.1f}")


async def real_time_monitoring_example():
    """Main real-time monitoring example."""

    logger.info("=== CAISO Real-Time Monitoring Example ===")

    # Create configuration optimized for real-time monitoring
    config = CAISOConnectorConfig(
        name="caiso_realtime_monitor",
        version="1.0.0",
        market="CAISO",
        mode="realtime",
        output_topic="ingestion.caiso.raw.v1",
        schema="schemas/raw_caiso_trade.avsc",
        # Real-time optimized settings
        caiso_timeout=30,
        caiso_rate_limit=300,  # Higher for real-time
        caiso_retry_attempts=3,
    )

    monitor = CAISORealTimeMonitor(config)

    # Nodes to monitor in real-time
    critical_nodes = [
        "TH_SP15_GEN-APND",  # Southern California - most liquid
        "TH_NP15_GEN-APND",  # Northern California
    ]

    # Set up signal handler for graceful shutdown
    def signal_handler():
        logger.info("Received shutdown signal")
        monitor.stop_monitoring()

    signal.signal(signal.SIGINT, lambda s, f: signal_handler())
    signal.signal(signal.SIGTERM, lambda s, f: signal_handler())

    try:
        # Run monitoring for 10 minutes (example)
        logger.info("Starting 10-minute monitoring session...")
        await asyncio.wait_for(
            monitor.start_monitoring(critical_nodes, "RTM"),
            timeout=600  # 10 minutes
        )

    except asyncio.TimeoutError:
        logger.info("Monitoring session completed (timeout reached)")
        monitor.stop_monitoring()
    except Exception as e:
        logger.error(f"Monitoring failed: {e}")
        monitor.stop_monitoring()
        raise


async def price_alert_example():
    """Example of real-time price alerting."""

    logger.info("=== CAISO Price Alerting Example ===")

    config = CAISOConnectorConfig(
        name="caiso_price_alerts",
        version="1.0.0",
        market="CAISO",
        mode="realtime",
        output_topic="ingestion.caiso.raw.v1",
        schema="schemas/raw_caiso_trade.avsc"
    )

    connector = CAISOConnector(config)

    # Alert thresholds (example values)
    price_thresholds = {
        "TH_SP15_GEN-APND": {"high": 75.0, "low": 0.0},
        "TH_NP15_GEN-APND": {"high": 70.0, "low": 0.0},
        "TH_ZP26_GEN-APND": {"high": 80.0, "low": 0.0}
    }

    alert_history = []

    try:
        while True:
            for node, thresholds in price_thresholds.items():
                try:
                    result = await connector.extract(
                        market_run_id="RTM",
                        node=node
                    )

                    if result.data:
                        latest_record = max(result.data, key=lambda x: x.get('occurred_at', 0))
                        price = latest_record.get('price')

                        if price is not None:
                            # Check alerts
                            if price > thresholds["high"]:
                                alert = {
                                    "timestamp": datetime.now(timezone.utc).isoformat(),
                                    "node": node,
                                    "alert_type": "high_price",
                                    "price": price,
                                    "threshold": thresholds["high"]
                                }
                                alert_history.append(alert)
                                logger.warning(f"HIGH PRICE ALERT: {node} - ${price} (threshold: ${thresholds['high']})")

                            elif price < thresholds["low"]:
                                alert = {
                                    "timestamp": datetime.now(timezone.utc).isoformat(),
                                    "node": node,
                                    "alert_type": "low_price",
                                    "price": price,
                                    "threshold": thresholds["low"]
                                }
                                alert_history.append(alert)
                                logger.warning(f"LOW PRICE ALERT: {node} - ${price} (threshold: ${thresholds['low']})")

                            else:
                                logger.info(f"Node {node}: ${price} (normal)")

                except Exception as e:
                    logger.error(f"Alert check failed for node {node}: {e}")

            # Wait before next check
            await asyncio.sleep(60)  # Check every minute

    except KeyboardInterrupt:
        logger.info("Price alerting stopped")

        # Summary
        logger.info(f"Generated {len(alert_history)} alerts:")
        for alert in alert_history[-5:]:  # Show last 5 alerts
            logger.info(f"  {alert['timestamp']}: {alert['alert_type']} on {alert['node']} - ${alert['price']}")


async def data_quality_monitoring_example():
    """Example of real-time data quality monitoring."""

    logger.info("=== CAISO Data Quality Monitoring Example ===")

    config = CAISOConnectorConfig(
        name="caiso_quality_monitor",
        version="1.0.0",
        market="CAISO",
        mode="realtime",
        output_topic="ingestion.caiso.raw.v1",
        schema="schemas/raw_caiso_trade.avsc"
    )

    connector = CAISOConnector(config)

    quality_metrics = {
        "total_records": 0,
        "valid_records": 0,
        "invalid_records": 0,
        "missing_fields": 0,
        "price_anomalies": 0
    }

    try:
        while True:
            result = await connector.extract(
                market_run_id="RTM",
                node="TH_SP15_GEN-APND"
            )

            quality_metrics["total_records"] += result.record_count

            for record in result.data:
                # Check data quality
                is_valid = True

                # Check required fields
                required_fields = ["event_id", "occurred_at", "price", "node"]
                for field in required_fields:
                    if not record.get(field):
                        quality_metrics["missing_fields"] += 1
                        is_valid = False

                # Check price anomalies (example: negative prices)
                if record.get("price", 0) < 0:
                    quality_metrics["price_anomalies"] += 1
                    logger.warning(f"Negative price detected: ${record.get('price')}")

                if is_valid:
                    quality_metrics["valid_records"] += 1
                else:
                    quality_metrics["invalid_records"] += 1

            # Calculate quality metrics
            if quality_metrics["total_records"] > 0:
                validity_rate = quality_metrics["valid_records"] / quality_metrics["total_records"]
                logger.info(f"Quality metrics - Validity: {validity_rate:.2%}, "
                           f"Anomalies: {quality_metrics['price_anomalies']}")

            await asyncio.sleep(60)  # Check every minute

    except KeyboardInterrupt:
        logger.info("Data quality monitoring stopped")

        # Final quality report
        logger.info("=== Final Quality Report ===")
        logger.info(f"Total records processed: {quality_metrics['total_records']}")
        logger.info(f"Valid records: {quality_metrics['valid_records']}")
        logger.info(f"Invalid records: {quality_metrics['invalid_records']}")
        logger.info(f"Missing fields: {quality_metrics['missing_fields']}")
        logger.info(f"Price anomalies: {quality_metrics['price_anomalies']}")

        if quality_metrics["total_records"] > 0:
            validity_rate = quality_metrics["valid_records"] / quality_metrics["total_records"]
            logger.info(f"Overall validity rate: {validity_rate:.2%}")


async def main():
    """Run all real-time monitoring examples."""

    try:
        # Choose which example to run
        logger.info("Select example to run:")
        logger.info("1. Basic real-time monitoring (10 minutes)")
        logger.info("2. Price alerting")
        logger.info("3. Data quality monitoring")
        logger.info("4. All examples (sequential)")

        choice = input("Enter choice (1-4): ").strip()

        if choice == "1":
            await real_time_monitoring_example()
        elif choice == "2":
            await price_alert_example()
        elif choice == "3":
            await data_quality_monitoring_example()
        elif choice == "4":
            await real_time_monitoring_example()
            print()
            await price_alert_example()
            print()
            await data_quality_monitoring_example()
        else:
            logger.error("Invalid choice")

    except Exception as e:
        logger.error(f"Real-time monitoring example failed: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
