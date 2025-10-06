#!/usr/bin/env python3
"""
CAISO Integration Example

This example demonstrates how the CAISO connector integrates with the broader
254Carbon Ingestion Platform, including normalization, enrichment, and Kafka publishing.
"""

import asyncio
import logging
import json
import sys
import os
from datetime import datetime, timezone

# Add project root to Python path for imports
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from connectors.caiso.connector import CAISOConnector
from connectors.caiso.config import CAISOConnectorConfig
from services.service_normalization.src.core.normalizer import NormalizationService
from connectors.base import ExtractionResult

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CAISOIntegrationPipeline:
    """Complete CAISO data pipeline from extraction to enrichment."""

    def __init__(self, caiso_config: CAISOConnectorConfig):
        self.caiso_connector = CAISOConnector(caiso_config)
        self.normalization_service = NormalizationService()
        self.pipeline_stats = {
            "extractions": 0,
            "normalizations": 0,
            "enrichments": 0,
            "kafka_messages": 0,
            "errors": 0
        }

    async def run_complete_pipeline(
        self,
        nodes: List[str],
        market_run_id: str = "DAM",
        time_window_hours: int = 1
    ) -> Dict[str, Any]:
        """
        Run the complete pipeline: Extract → Normalize → Enrich → Publish

        Args:
            nodes: List of nodes to process
            market_run_id: Market run ID
            time_window_hours: Hours of data to process

        Returns:
            Pipeline execution results
        """
        logger.info(f"Starting complete pipeline for {len(nodes)} nodes, {time_window_hours}h window")

        start_time = datetime.now(timezone.utc)
        end_time = start_time - timedelta(hours=time_window_hours)

        pipeline_results = {
            "extraction_results": [],
            "normalized_data": [],
            "enriched_data": [],
            "published_messages": 0,
            "errors": []
        }

        try:
            # Step 1: Extract data from CAISO
            logger.info("Step 1: Data Extraction")
            for node in nodes:
                try:
                    extraction_result = await self.caiso_connector.extract(
                        start=end_time.strftime("%Y-%m-%dT%H:%M:%S"),
                        end=start_time.strftime("%Y-%m-%dT%H:%M:%S"),
                        market_run_id=market_run_id,
                        node=node
                    )

                    self.pipeline_stats["extractions"] += 1
                    pipeline_results["extraction_results"].append({
                        "node": node,
                        "records": extraction_result.record_count,
                        "metadata": extraction_result.metadata
                    })

                    logger.info(f"✓ Extracted {extraction_result.record_count} records for node {node}")

                    if extraction_result.record_count == 0:
                        logger.warning(f"No data extracted for node {node}")
                        continue

                    # Step 2: Normalize the data
                    logger.info("Step 2: Data Normalization")
                    normalized_records = await self.normalization_service.normalize_batch(
                        extraction_result.data
                    )

                    self.pipeline_stats["normalizations"] += len(normalized_records)
                    pipeline_results["normalized_data"].extend(normalized_records)

                    logger.info(f"✓ Normalized {len(normalized_records)} records")

                    # Step 3: Simulate enrichment (would use enrichment service in production)
                    logger.info("Step 3: Data Enrichment")
                    enriched_records = await self._simulate_enrichment(normalized_records)
                    self.pipeline_stats["enrichments"] += len(enriched_records)
                    pipeline_results["enriched_data"].extend(enriched_records)

                    logger.info(f"✓ Enriched {len(enriched_records)} records")

                    # Step 4: Simulate Kafka publishing
                    logger.info("Step 4: Kafka Publishing")
                    published_count = await self._simulate_kafka_publish(enriched_records)
                    self.pipeline_stats["kafka_messages"] += published_count
                    pipeline_results["published_messages"] += published_count

                    logger.info(f"✓ Published {published_count} messages to Kafka")

                except Exception as e:
                    error_msg = f"Pipeline failed for node {node}: {e}"
                    logger.error(error_msg)
                    self.pipeline_stats["errors"] += 1
                    pipeline_results["errors"].append(error_msg)

        except Exception as e:
            error_msg = f"Pipeline execution failed: {e}"
            logger.error(error_msg)
            self.pipeline_stats["errors"] += 1
            pipeline_results["errors"].append(error_msg)

        # Calculate pipeline metrics
        total_time = (datetime.now(timezone.utc) - start_time).total_seconds()
        pipeline_results["total_time_seconds"] = total_time
        pipeline_results["records_per_second"] = len(pipeline_results["enriched_data"]) / max(1, total_time)

        logger.info(f"Pipeline completed in {total_time:.2f}s")
        logger.info(f"Processed {len(pipeline_results['enriched_data'])} records")
        logger.info(f"Rate: {pipeline_results['records_per_second']:.2f} records/second")

        return pipeline_results

    async def _simulate_enrichment(self, records: List[Dict]) -> List[Dict]:
        """Simulate data enrichment (would use enrichment service in production)."""
        enriched_records = []

        for record in records:
            enriched = record.copy()

            # Add simulated enrichment data
            enriched.update({
                "enriched_at": datetime.now(timezone.utc).isoformat(),
                "data_quality_score": 0.95,
                "geospatial_data": {
                    "latitude": 34.0522,  # Los Angeles coordinates
                    "longitude": -118.2437,
                    "timezone": "America/Los_Angeles"
                },
                "market_metadata": {
                    "market_type": "energy",
                    "trading_hub": record.get("node", "unknown"),
                    "currency": "USD"
                }
            })

            enriched_records.append(enriched)

        return enriched_records

    async def _simulate_kafka_publish(self, records: List[Dict]) -> int:
        """Simulate Kafka publishing (would use actual Kafka producer in production)."""
        published_count = 0

        for record in records:
            try:
                # In production, this would be:
                # await kafka_producer.send(
                #     topic=self.caiso_connector.config.output_topic,
                #     value=record,
                #     key=record["event_id"]
                # )

                # Simulate publishing
                logger.debug(f"Publishing record {record['event_id']} to Kafka")
                published_count += 1

                # Small delay to simulate network I/O
                await asyncio.sleep(0.001)

            except Exception as e:
                logger.error(f"Failed to publish record {record.get('event_id')}: {e}")

        return published_count


async def integration_example():
    """Main integration example."""

    logger.info("=== CAISO Integration Pipeline Example ===")

    # Create CAISO configuration
    caiso_config = CAISOConnectorConfig(
        name="caiso_integration_example",
        version="1.0.0",
        market="CAISO",
        mode="batch",
        output_topic="ingestion.caiso.raw.v1",
        schema="schemas/raw_caiso_trade.avsc"
    )

    pipeline = CAISOIntegrationPipeline(caiso_config)

    # California nodes to process
    california_nodes = [
        "TH_SP15_GEN-APND",  # Southern California (Los Angeles area)
        "TH_NP15_GEN-APND",  # Northern California (San Francisco area)
        "TH_ZP26_GEN-APND"   # Arizona (Phoenix area)
    ]

    try:
        # Run the complete pipeline
        results = await pipeline.run_complete_pipeline(
            nodes=california_nodes,
            market_run_id="DAM",
            time_window_hours=2  # Process 2 hours of data
        )

        # Display results
        logger.info("=== Pipeline Results ===")
        logger.info(f"Extraction results: {len(results['extraction_results'])} nodes processed")
        logger.info(f"Normalized records: {len(results['normalized_data'])}")
        logger.info(f"Enriched records: {len(results['enriched_data'])}")
        logger.info(f"Kafka messages: {results['published_messages']}")
        logger.info(f"Total time: {results['total_time_seconds']:.2f}s")
        logger.info(f"Processing rate: {results['records_per_second']:.2f} records/second")

        if results["errors"]:
            logger.warning(f"Errors encountered: {len(results['errors'])}")
            for error in results["errors"][:3]:  # Show first 3 errors
                logger.warning(f"  - {error}")

        # Show sample of enriched data
        if results["enriched_data"]:
            sample = results["enriched_data"][0]
            logger.info("Sample enriched record:")
            logger.info(f"  Event ID: {sample.get('event_id')}")
            logger.info(f"  Node: {sample.get('node')}")
            logger.info(f"  Price: ${sample.get('price')}")
            logger.info(f"  Enriched at: {sample.get('enriched_at')}")
            logger.info(f"  Quality score: {sample.get('data_quality_score')}")

        # Pipeline statistics
        logger.info("=== Pipeline Statistics ===")
        for key, value in pipeline.pipeline_stats.items():
            logger.info(f"{key}: {value}")

        return results

    except Exception as e:
        logger.error(f"Integration pipeline failed: {e}")
        raise


async def multi_market_comparison_example():
    """Example comparing data across multiple market runs."""

    logger.info("=== Multi-Market Comparison Example ===")

    caiso_config = CAISOConnectorConfig(
        name="caiso_multi_market",
        version="1.0.0",
        market="CAISO",
        mode="batch",
        output_topic="ingestion.caiso.raw.v1",
        schema="schemas/raw_caiso_trade.avsc"
    )

    connector = CAISOConnector(caiso_config)

    # Compare Day-Ahead Market (DAM) vs Real-Time Market (RTM)
    nodes = ["TH_SP15_GEN-APND"]
    time_window = "2024-01-01T12:00:00"  # Noon

    market_comparison = {}

    for market_run_id in ["DAM", "RTM"]:
        try:
            logger.info(f"Extracting {market_run_id} data...")

            result = await connector.extract(
                start=time_window,
                end=time_window,
                market_run_id=market_run_id,
                node=nodes[0]
            )

            market_comparison[market_run_id] = {
                "records": result.record_count,
                "sample_price": None,
                "metadata": result.metadata
            }

            if result.data:
                # Find the price for this specific time
                for record in result.data:
                    if record.get("market_run_id") == market_run_id:
                        market_comparison[market_run_id]["sample_price"] = record.get("price")
                        break

            logger.info(f"{market_run_id}: {result.record_count} records, sample price: ${market_comparison[market_run_id]['sample_price']}")

        except Exception as e:
            logger.error(f"Failed to extract {market_run_id} data: {e}")
            market_comparison[market_run_id] = {"records": 0, "error": str(e)}

    # Analyze comparison
    logger.info("=== Market Comparison Analysis ===")

    dam_data = market_comparison.get("DAM", {})
    rtm_data = market_comparison.get("RTM", {})

    if dam_data.get("sample_price") and rtm_data.get("sample_price"):
        price_diff = abs(dam_data["sample_price"] - rtm_data["sample_price"])
        logger.info(f"Price difference (DAM vs RTM): ${price_diff:.2f}")

        if price_diff > 10:  # Arbitrary threshold
            logger.warning(f"Significant price difference detected: ${price_diff:.2f}")

    logger.info(f"DAM records: {dam_data.get('records', 0)}")
    logger.info(f"RTM records: {rtm_data.get('records', 0)}")


async def performance_monitoring_example():
    """Example of monitoring CAISO pipeline performance."""

    logger.info("=== Performance Monitoring Example ===")

    class CAISOPerformanceMonitor:
        """Monitor CAISO pipeline performance metrics."""

        def __init__(self):
            self.metrics = {
                "extraction_times": [],
                "normalization_times": [],
                "enrichment_times": [],
                "total_pipeline_times": [],
                "records_processed": [],
                "errors": []
            }

        def record_extraction_time(self, duration: float):
            self.metrics["extraction_times"].append(duration)

        def record_normalization_time(self, duration: float):
            self.metrics["normalization_times"].append(duration)

        def record_enrichment_time(self, duration: float):
            self.metrics["enrichment_times"].append(duration)

        def record_pipeline_time(self, duration: float, records: int):
            self.metrics["total_pipeline_times"].append(duration)
            self.metrics["records_processed"].append(records)

        def get_performance_summary(self) -> Dict[str, Any]:
            """Calculate performance statistics."""
            if not self.metrics["total_pipeline_times"]:
                return {"error": "No data collected"}

            return {
                "avg_extraction_time": sum(self.metrics["extraction_times"]) / len(self.metrics["extraction_times"]),
                "avg_normalization_time": sum(self.metrics["normalization_times"]) / len(self.metrics["normalization_times"]),
                "avg_enrichment_time": sum(self.metrics["enrichment_times"]) / len(self.metrics["enrichment_times"]),
                "avg_total_time": sum(self.metrics["total_pipeline_times"]) / len(self.metrics["total_pipeline_times"]),
                "total_records": sum(self.metrics["records_processed"]),
                "avg_records_per_second": sum(self.metrics["records_processed"]) / sum(self.metrics["total_pipeline_times"]),
                "error_count": len(self.metrics["errors"])
            }

    monitor = CAISOPerformanceMonitor()
    caiso_config = CAISOConnectorConfig(
        name="caiso_performance_test",
        version="1.0.0",
        market="CAISO",
        mode="batch",
        output_topic="ingestion.caiso.raw.v1",
        schema="schemas/raw_caiso_trade.avsc"
    )

    connector = CAISOConnector(caiso_config)
    normalization_service = NormalizationService()

    logger.info("Running performance tests...")

    # Test with different data volumes
    test_scenarios = [
        {"nodes": 1, "hours": 1},
        {"nodes": 2, "hours": 1},
        {"nodes": 1, "hours": 2},
    ]

    for scenario in test_scenarios:
        logger.info(f"Testing scenario: {scenario['nodes']} nodes, {scenario['hours']} hours")

        start_time = datetime.now(timezone.utc)

        try:
            result = await connector.extract(
                start=(start_time - timedelta(hours=scenario['hours'])).strftime("%Y-%m-%dT%H:%M:%S"),
                end=start_time.strftime("%Y-%m-%dT%H:%M:%S"),
                market_run_id="DAM",
                node="TH_SP15_GEN-APND"
            )

            extraction_time = (datetime.now(timezone.utc) - start_time).total_seconds()
            monitor.record_extraction_time(extraction_time)

            if result.record_count > 0:
                # Normalization timing
                norm_start = datetime.now(timezone.utc)
                normalized = await normalization_service.normalize_batch(result.data)
                norm_time = (datetime.now(timezone.utc) - norm_start).total_seconds()
                monitor.record_normalization_time(norm_time)

                # Enrichment timing (simulated)
                enrich_start = datetime.now(timezone.utc)
                # Simulate enrichment processing
                await asyncio.sleep(0.01 * len(normalized))  # Simulate processing time
                enrich_time = (datetime.now(timezone.utc) - enrich_start).total_seconds()
                monitor.record_enrichment_time(enrich_time)

                # Total pipeline time
                total_time = extraction_time + norm_time + enrich_time
                monitor.record_pipeline_time(total_time, result.record_count)

                logger.info(f"  Records: {result.record_count}")
                logger.info(f"  Extraction: {extraction_time:.3f}s")
                logger.info(f"  Normalization: {norm_time:.3f}s")
                logger.info(f"  Enrichment: {enrich_time:.3f}s")
                logger.info(f"  Total: {total_time:.3f}s")
                logger.info(f"  Rate: {result.record_count/total_time:.1f} records/second")
            else:
                logger.warning("  No records extracted")

        except Exception as e:
            logger.error(f"  Error: {e}")
            monitor.metrics["errors"].append(str(e))

        # Brief pause between scenarios
        await asyncio.sleep(1)

    # Performance summary
    summary = monitor.get_performance_summary()
    logger.info("=== Performance Summary ===")
    for key, value in summary.items():
        if isinstance(value, float):
            logger.info(f"{key}: {value:.3f}")
        else:
            logger.info(f"{key}: {value}")


async def main():
    """Run all integration examples."""

    try:
        # Complete pipeline integration
        await integration_example()
        print()

        # Multi-market comparison
        await multi_market_comparison_example()
        print()

        # Performance monitoring
        await performance_monitoring_example()

        logger.info("All integration examples completed!")

    except Exception as e:
        logger.error(f"Integration example failed: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
