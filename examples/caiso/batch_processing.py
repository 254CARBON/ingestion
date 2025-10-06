#!/usr/bin/env python3
"""
CAISO Batch Processing Example

This example demonstrates how to process large amounts of historical CAISO data
in batches for backfilling or regular historical updates.
"""

#!/usr/bin/env python3
"""
CAISO Batch Processing Example

This example demonstrates how to process large amounts of historical CAISO data
in batches for backfilling or regular historical updates.
"""

import asyncio
import logging
import sys
import os
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any

# Add project root to Python path for imports
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from connectors.caiso.connector import CAISOConnector
from connectors.caiso.config import CAISOConnectorConfig
from connectors.base import ExtractionResult

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CAISOBatchProcessor:
    """Batch processor for CAISO historical data."""

    def __init__(self, config: CAISOConnectorConfig):
        self.connector = CAISOConnector(config)
        self.batch_size_hours = 24  # Process 24 hours at a time
        self.max_retries = 3

    async def process_date_range(
        self,
        start_date: datetime,
        end_date: datetime,
        nodes: List[str],
        market_run_id: str = "DAM"
    ) -> List[ExtractionResult]:
        """
        Process a date range in batches.

        Args:
            start_date: Start date for processing
            end_date: End date for processing
            nodes: List of nodes to process
            market_run_id: Market run ID (DAM/RTM)

        Returns:
            List of extraction results
        """
        logger.info(f"Processing date range: {start_date} to {end_date}")
        logger.info(f"Nodes: {nodes}")
        logger.info(f"Market run: {market_run_id}")

        results = []
        current_date = start_date

        while current_date < end_date:
            batch_end = min(current_date + timedelta(hours=self.batch_size_hours), end_date)

            logger.info(f"Processing batch: {current_date} to {batch_end}")

            # Process each node for this time window
            for node in nodes:
                for attempt in range(self.max_retries):
                    try:
                        result = await self.connector.extract(
                            start=current_date.strftime("%Y-%m-%dT%H:%M:%S"),
                            end=batch_end.strftime("%Y-%m-%dT%H:%M:%S"),
                            market_run_id=market_run_id,
                            node=node
                        )

                        logger.info(f"Node {node}: {result.record_count} records extracted")
                        results.append(result)

                        # Small delay between nodes to respect rate limits
                        await asyncio.sleep(1)
                        break  # Success, exit retry loop

                    except Exception as e:
                        logger.warning(f"Attempt {attempt + 1} failed for node {node}: {e}")
                        if attempt < self.max_retries - 1:
                            # Exponential backoff
                            delay = (2 ** attempt) * 5
                            logger.info(f"Retrying in {delay} seconds...")
                            await asyncio.sleep(delay)
                        else:
                            logger.error(f"Max retries exceeded for node {node}")

            current_date = batch_end

        return results

    async def process_multiple_market_runs(
        self,
        date: datetime,
        nodes: List[str],
        market_runs: List[str] = None
    ) -> Dict[str, List[ExtractionResult]]:
        """
        Process multiple market runs for a single day.

        Args:
            date: Date to process
            nodes: List of nodes to process
            market_runs: List of market run IDs

        Returns:
            Dictionary mapping market runs to results
        """
        if market_runs is None:
            market_runs = ["DAM", "RTM"]

        results = {}

        for market_run_id in market_runs:
            logger.info(f"Processing market run: {market_run_id}")

            try:
                # Process full day for this market run
                day_results = await self.process_date_range(
                    start_date=date.replace(hour=0, minute=0, second=0, microsecond=0),
                    end_date=date.replace(hour=23, minute=59, second=59, microsecond=999999),
                    nodes=nodes,
                    market_run_id=market_run_id
                )

                results[market_run_id] = day_results

                # Brief pause between market runs
                await asyncio.sleep(2)

            except Exception as e:
                logger.error(f"Failed to process market run {market_run_id}: {e}")
                results[market_run_id] = []

        return results


async def batch_processing_example():
    """Main batch processing example."""

    logger.info("=== CAISO Batch Processing Example ===")

    # Create configuration optimized for batch processing
    config = CAISOConnectorConfig(
        name="caiso_batch_processor",
        version="1.0.0",
        market="CAISO",
        mode="batch",
        output_topic="ingestion.caiso.raw.v1",
        schema="schemas/raw_caiso_trade.avsc",
        # Batch-optimized settings
        caiso_timeout=60,  # Longer timeout for large batches
        caiso_rate_limit=50,  # Lower rate limit for respectful processing
        caiso_retry_attempts=5,  # More retries for reliability
    )

    processor = CAISOBatchProcessor(config)

    # Example 1: Process a single day for multiple nodes
    logger.info("Example 1: Single day, multiple nodes")
    target_date = datetime.now(timezone.utc) - timedelta(days=7)  # 7 days ago

    nodes = [
        "TH_SP15_GEN-APND",  # Southern California
        "TH_NP15_GEN-APND",  # Northern California
        "TH_ZP26_GEN-APND"   # Arizona
    ]

    try:
        results = await processor.process_date_range(
            start_date=target_date.replace(hour=0, minute=0, second=0, microsecond=0),
            end_date=target_date.replace(hour=23, minute=59, second=59, microsecond=999999),
            nodes=nodes,
            market_run_id="DAM"
        )

        total_records = sum(result.record_count for result in results)
        logger.info(f"Processed {len(results)} batches with {total_records} total records")

    except Exception as e:
        logger.error(f"Batch processing failed: {e}")

    # Example 2: Process multiple market runs for a day
    logger.info("Example 2: Single day, multiple market runs")

    try:
        market_results = await processor.process_multiple_market_runs(
            date=target_date,
            nodes=nodes[:2],  # Just first 2 nodes for this example
            market_runs=["DAM", "RTM"]
        )

        for market_run, results in market_results.items():
            total_records = sum(result.record_count for result in results)
            logger.info(f"Market run {market_run}: {total_records} records")

    except Exception as e:
        logger.error(f"Multi-market processing failed: {e}")

    # Example 3: Process a week of data (for backfilling)
    logger.info("Example 3: Week-long backfill")

    try:
        week_start = target_date - timedelta(days=7)
        week_end = target_date

        logger.info(f"Backfilling from {week_start.date()} to {week_end.date()}")

        # Process one day at a time to avoid memory issues
        current_day = week_start
        daily_totals = []

        while current_day < week_end:
            day_results = await processor.process_date_range(
                start_date=current_day.replace(hour=0, minute=0, second=0, microsecond=0),
                end_date=current_day.replace(hour=23, minute=59, second=59, microsecond=999999),
                nodes=[nodes[0]],  # Just one node for backfill example
                market_run_id="DAM"
            )

            day_total = sum(result.record_count for result in day_results)
            daily_totals.append((current_day.date(), day_total))

            logger.info(f"Day {current_day.date()}: {day_total} records")

            current_day += timedelta(days=1)

            # Brief pause between days
            await asyncio.sleep(1)

        total_week_records = sum(count for _, count in daily_totals)
        logger.info(f"Week total: {total_week_records} records")
        logger.info(f"Daily breakdown: {daily_totals}")

    except Exception as e:
        logger.error(f"Backfill processing failed: {e}")


async def production_batch_example():
    """Production-style batch processing with error handling."""

    logger.info("=== Production Batch Processing Example ===")

    config = CAISOConnectorConfig(
        name="caiso_production_batch",
        version="1.0.0",
        market="CAISO",
        mode="batch",
        output_topic="ingestion.caiso.raw.v1",
        schema="schemas/raw_caiso_trade.avsc",
        # Production settings
        caiso_timeout=90,
        caiso_rate_limit=200,
        caiso_retry_attempts=8,
    )

    processor = CAISOBatchProcessor(config)

    # Production scenario: Process last 30 days for compliance reporting
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=30)

    logger.info(f"Processing compliance period: {start_date.date()} to {end_date.date()}")

    # Critical nodes for compliance reporting
    compliance_nodes = [
        "TH_SP15_GEN-APND",
        "TH_NP15_GEN-APND",
        "TH_ZP26_GEN-APND"
    ]

    try:
        results = await processor.process_date_range(
            start_date=start_date,
            end_date=end_date,
            nodes=compliance_nodes,
            market_run_id="DAM"
        )

        # Analyze results
        successful_batches = len([r for r in results if r.record_count > 0])
        total_records = sum(r.record_count for r in results)

        logger.info("Compliance processing complete:")
        logger.info(f"  - Successful batches: {successful_batches}/{len(results)}")
        logger.info(f"  - Total records: {total_records}")
        logger.info(f"  - Average per batch: {total_records / len(results):.1f}")

        # Check for data gaps
        if successful_batches < len(results):
            failed_batches = len(results) - successful_batches
            logger.warning(f"Data gaps detected: {failed_batches} failed batches")

        return results

    except Exception as e:
        logger.error(f"Production batch processing failed: {e}")
        raise


async def main():
    """Run all batch processing examples."""

    try:
        # Basic batch processing
        await batch_processing_example()
        print()

        # Production batch processing
        await production_batch_example()

        logger.info("All batch processing examples completed!")

    except Exception as e:
        logger.error(f"Batch processing example failed: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
