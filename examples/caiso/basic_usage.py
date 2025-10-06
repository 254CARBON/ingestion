#!/usr/bin/env python3
"""
Basic CAISO Connector Usage Example

This example demonstrates the simplest way to extract CAISO market data
using the 254Carbon Ingestion Platform.
"""

import asyncio
import logging
import sys
import os
from datetime import datetime, timezone, timedelta

# Add project root to Python path for imports
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from connectors.caiso.connector import CAISOConnector
from connectors.caiso.config import CAISOConnectorConfig

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def basic_extraction_example():
    """Example of basic CAISO data extraction."""

    logger.info("Starting basic CAISO extraction example")

    # Create a basic configuration
    config = CAISOConnectorConfig(
        name="caiso_basic_example",
        version="1.0.0",
        market="CAISO",
        mode="batch",
        output_topic="ingestion.caiso.raw.v1",
        schema="schemas/raw_caiso_trade.avsc",
        # Use default CAISO settings
    )

    # Create the connector
    connector = CAISOConnector(config)

    # Check connector status
    health = connector.get_health_status()
    logger.info(f"Connector health: {health}")

    try:
        # Extract data for a specific time window and node
        start_time = "2024-01-01T00:00:00"
        end_time = "2024-01-01T01:00:00"

        logger.info(f"Extracting CAISO data from {start_time} to {end_time}")

        result = await connector.extract(
            start=start_time,
            end=end_time,
            market_run_id="DAM",  # Day-Ahead Market
            node="TH_SP15_GEN-APND"  # Southern California generation node
        )

        logger.info(f"Extraction completed: {result.record_count} records extracted")

        if result.data:
            # Show sample of extracted data
            sample_record = result.data[0]
            logger.info("Sample record keys: %s", list(sample_record.keys()))
            logger.info("Sample record: %s", {
                k: v for k, v in sample_record.items()
                if k in ['event_id', 'node', 'price', 'market', 'occurred_at']
            })

            # Show metadata
            logger.info("Extraction metadata: %s", result.metadata)

        return result

    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        raise


async def multiple_nodes_example():
    """Example of extracting data from multiple nodes."""

    logger.info("Starting multi-node extraction example")

    config = CAISOConnectorConfig(
        name="caiso_multi_node",
        version="1.0.0",
        market="CAISO",
        mode="batch",
        output_topic="ingestion.caiso.raw.v1",
        schema="schemas/raw_caiso_trade.avsc"
    )

    connector = CAISOConnector(config)

    # Nodes to extract data for
    nodes = [
        "TH_SP15_GEN-APND",  # Southern California
        "TH_NP15_GEN-APND",  # Northern California
        "TH_ZP26_GEN-APND"   # Arizona
    ]

    start_time = "2024-01-01T00:00:00"
    end_time = "2024-01-01T01:00:00"

    all_results = []

    for node in nodes:
        logger.info(f"Extracting data for node: {node}")

        try:
            result = await connector.extract(
                start=start_time,
                end=end_time,
                market_run_id="DAM",
                node=node
            )

            logger.info(f"Node {node}: {result.record_count} records")
            all_results.append((node, result))

        except Exception as e:
            logger.error(f"Failed to extract data for node {node}: {e}")

    # Summary
    total_records = sum(result.record_count for _, result in all_results)
    logger.info(f"Total records extracted: {total_records}")

    return all_results


async def real_time_extraction_example():
    """Example of real-time data extraction."""

    logger.info("Starting real-time extraction example")

    config = CAISOConnectorConfig(
        name="caiso_realtime",
        version="1.0.0",
        market="CAISO",
        mode="realtime",
        output_topic="ingestion.caiso.raw.v1",
        schema="schemas/raw_caiso_trade.avsc"
    )

    connector = CAISOConnector(config)

    try:
        # Extract real-time data (uses current time window automatically)
        result = await connector.extract(
            market_run_id="RTM",  # Real-Time Market
            node="TH_SP15_GEN-APND"
        )

        logger.info(f"Real-time extraction: {result.record_count} records")

        if result.data:
            # Show the most recent data point
            latest_record = max(result.data, key=lambda x: x.get('occurred_at', 0))
            logger.info("Latest data point: %s", {
                k: v for k, v in latest_record.items()
                if k in ['node', 'price', 'occurred_at', 'market_run_id']
            })

        return result

    except Exception as e:
        logger.error(f"Real-time extraction failed: {e}")
        raise


async def error_handling_example():
    """Example of proper error handling."""

    logger.info("Starting error handling example")

    config = CAISOConnectorConfig(
        name="caiso_error_example",
        version="1.0.0",
        market="CAISO",
        mode="batch",
        output_topic="ingestion.caiso.raw.v1",
        schema="schemas/raw_caiso_trade.avsc"
    )

    connector = CAISOConnector(config)

    # Test with invalid node
    try:
        result = await connector.extract(
            start="2024-01-01T00:00:00",
            end="2024-01-01T01:00:00",
            market_run_id="DAM",
            node="INVALID_NODE_12345"  # This node doesn't exist
        )

        logger.warning(f"Unexpected success with invalid node: {result.record_count} records")

    except Exception as e:
        logger.info(f"Expected error with invalid node: {type(e).__name__}: {e}")

    # Test with valid node but invalid time range
    try:
        # Future date that may not have data
        future_date = (datetime.now(timezone.utc) + timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%S")

        result = await connector.extract(
            start=future_date,
            end=future_date,
            market_run_id="DAM",
            node="TH_SP15_GEN-APND"
        )

        logger.info(f"Future date extraction: {result.record_count} records")

    except Exception as e:
        logger.info(f"Expected error with future date: {type(e).__name__}: {e}")


async def main():
    """Run all examples."""

    logger.info("=== CAISO Connector Examples ===")

    try:
        # Basic extraction
        await basic_extraction_example()
        print()

        # Multiple nodes
        await multiple_nodes_example()
        print()

        # Real-time extraction
        await real_time_extraction_example()
        print()

        # Error handling
        await error_handling_example()

        logger.info("All examples completed successfully!")

    except Exception as e:
        logger.error(f"Example failed: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
