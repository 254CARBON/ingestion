"""
CAISO connector integration example.

This example demonstrates end-to-end integration of the CAISO connector
with the ingestion pipeline, including extraction, transformation, and
publishing to Kafka.
"""

import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, Any

from connectors.caiso import CAISOConnector, CAISOConnectorConfig


class MockKafkaProducer:
    """Mock Kafka producer for testing."""
    
    def __init__(self):
        self.published_messages = []
    
    async def publish(self, topic: str, message: Dict[str, Any]) -> bool:
        """Publish message to topic."""
        self.published_messages.append({
            "topic": topic,
            "message": message,
            "timestamp": datetime.now(timezone.utc).isoformat()
        })
        print(f"Published to {topic}: {len(message)} records")
        return True
    
    def get_stats(self) -> Dict[str, Any]:
        """Get publishing statistics."""
        return {
            "total_messages": len(self.published_messages),
            "topics": list(set(msg["topic"] for msg in self.published_messages))
        }


async def main():
    """Main integration example function."""
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    # Create CAISO connector configuration
    config = CAISOConnectorConfig(
        name="caiso",
        version="1.0.0",
        market="CAISO",
        mode="batch",
        enabled=True,
        output_topic="ingestion.caiso.raw.v1",
        schema="schemas/raw_caiso_trade.avsc",
        retries=3,
        backoff_seconds=30,
        tenant_strategy="single",
        transforms=["sanitize_numeric", "standardize_timezone"],
        owner="platform",
        description="CAISO market data connector",
        tags=["market-data", "energy", "trading"],
        # CAISO-specific settings
        caiso_base_url="https://oasis.caiso.com/oasisapi",
        caiso_timeout=30,
        caiso_rate_limit=100,
        caiso_retry_attempts=3,
        caiso_backoff_factor=2.0,
        caiso_user_agent="254Carbon/1.0",
        oasis_version="12",
        oasis_result_format="6",
        default_market_run_id="DAM",
        default_node="TH_SP15_GEN-APND"
    )
    
    # Create connector instance
    connector = CAISOConnector(config)
    
    # Create mock Kafka producer
    kafka_producer = MockKafkaProducer()
    
    try:
        print("Starting CAISO connector integration example...")
        
        # Define extraction parameters
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(hours=6)  # Last 6 hours
        
        print(f"Extracting data from {start_time} to {end_time}")
        
        # Step 1: Extract data
        print("\n=== Step 1: Data Extraction ===")
        extraction_result = await connector.extract(
            start=start_time,
            end=end_time,
            market_run_id="DAM",
            node="TH_SP15_GEN-APND"
        )
        
        print(f"Extraction completed:")
        print(f"  Records extracted: {extraction_result.record_count}")
        print(f"  Query metadata: {extraction_result.metadata}")
        
        if not extraction_result.data:
            print("No data extracted. Exiting.")
            return
        
        # Step 2: Transform data
        print("\n=== Step 2: Data Transformation ===")
        transformation_result = await connector.transform(extraction_result)
        
        print(f"Transformation completed:")
        print(f"  Records transformed: {transformation_result.record_count}")
        print(f"  Validation errors: {len(transformation_result.validation_errors)}")
        
        if transformation_result.validation_errors:
            print("  Validation errors:")
            for error in transformation_result.validation_errors[:5]:  # Show first 5
                print(f"    - {error}")
        
        # Step 3: Load data (publish to Kafka)
        print("\n=== Step 3: Data Loading ===")
        load_result = await connector.load(transformation_result)
        
        print(f"Load completed:")
        print(
            "  Records loaded: "
            f"{load_result.records_published}/{load_result.records_attempted}"
        )
        print(f"  Failed publishes: {load_result.records_failed}")
        print(f"  Load success: {load_result.success}")
        print(f"  Load metadata: {load_result.metadata}")
        
        # Step 4: Verify data quality
        print("\n=== Step 4: Data Quality Verification ===")
        await verify_data_quality(transformation_result.data)
        
        # Step 5: Simulate downstream processing
        print("\n=== Step 5: Downstream Processing Simulation ===")
        await simulate_downstream_processing(transformation_result.data)
        
        # Step 6: Generate summary report
        print("\n=== Step 6: Summary Report ===")
        await generate_summary_report(
            extraction_result,
            transformation_result,
            load_result,
            kafka_producer
        )
        
        print("\nCAISO connector integration example completed successfully!")
        
    except Exception as e:
        print(f"Error in CAISO connector integration example: {e}")
        logging.error(f"CAISO connector integration example failed: {e}")
        raise
    
    finally:
        # Cleanup
        await connector.cleanup()


async def verify_data_quality(data: list) -> None:
    """Verify data quality of transformed records."""
    if not data:
        print("No data to verify.")
        return
    
    print("Data quality verification:")
    
    # Check required fields
    required_fields = ["event_id", "occurred_at", "tenant_id", "market", "data_type"]
    missing_fields = []
    
    for record in data[:10]:  # Check first 10 records
        for field in required_fields:
            if field not in record or record[field] is None:
                missing_fields.append(field)
    
    if missing_fields:
        print(f"  Missing required fields: {set(missing_fields)}")
    else:
        print("  All required fields present")
    
    # Check data types
    type_issues = []
    for record in data[:10]:
        if "price" in record and record["price"] is not None:
            try:
                float(record["price"])
            except (ValueError, TypeError):
                type_issues.append("price")
    
    if type_issues:
        print(f"  Data type issues: {set(type_issues)}")
    else:
        print("  Data types are valid")
    
    # Check timestamp format
    timestamp_issues = []
    for record in data[:10]:
        if "occurred_at" in record and record["occurred_at"] is not None:
            try:
                int(record["occurred_at"])
            except (ValueError, TypeError):
                timestamp_issues.append("occurred_at")
    
    if timestamp_issues:
        print(f"  Timestamp issues: {set(timestamp_issues)}")
    else:
        print("  Timestamps are valid")


async def simulate_downstream_processing(data: list) -> None:
    """Simulate downstream processing of the data."""
    if not data:
        print("No data to process.")
        return
    
    print("Simulating downstream processing:")
    
    # Simulate normalization service
    print("  Normalization service: Processing records...")
    await asyncio.sleep(0.1)  # Simulate processing time
    print(f"    Normalized {len(data)} records")
    
    # Simulate enrichment service
    print("  Enrichment service: Adding taxonomy and semantic tags...")
    await asyncio.sleep(0.1)  # Simulate processing time
    print(f"    Enriched {len(data)} records")
    
    # Simulate aggregation service
    print("  Aggregation service: Generating OHLC bars and metrics...")
    await asyncio.sleep(0.1)  # Simulate processing time
    print(f"    Generated aggregates for {len(data)} records")
    
    print("  Downstream processing simulation completed")


async def generate_summary_report(
    extraction_result,
    transformation_result,
    load_result,
    kafka_producer
) -> None:
    """Generate a summary report of the integration."""
    print("Integration Summary Report:")
    print("=" * 50)
    
    # Extraction summary
    print(f"Extraction:")
    print(f"  Records extracted: {extraction_result.record_count}")
    print(f"  Source: {extraction_result.metadata.get('source', 'unknown')}")
    print(f"  Query: {extraction_result.metadata.get('query', {})}")
    
    # Transformation summary
    print(f"\nTransformation:")
    print(f"  Records transformed: {transformation_result.record_count}")
    print(f"  Validation errors: {len(transformation_result.validation_errors)}")
    print(f"  Transforms applied: {transformation_result.metadata.get('transforms_applied', [])}")
    
    # Load summary
    print(f"\nLoad:")
    print(
        "  Records loaded: "
        f"{load_result.records_published}/{load_result.records_attempted}"
    )
    print(f"  Failed publishes: {load_result.records_failed}")
    print(f"  Load success: {load_result.success}")
    print(f"  Load method: {load_result.metadata.get('load_method', 'unknown')}")
    if load_result.errors:
        print(f"  Load errors (sample): {load_result.errors[:3]}")
    
    # Kafka summary
    kafka_stats = kafka_producer.get_stats()
    print(f"\nKafka Publishing:")
    print(f"  Total messages: {kafka_stats['total_messages']}")
    print(f"  Topics used: {kafka_stats['topics']}")
    
    # Overall summary
    print(f"\nOverall:")
    transform_success = (
        transformation_result.record_count / max(1, extraction_result.record_count)
    ) * 100
    load_success = (
        load_result.records_published / max(1, load_result.records_attempted)
    ) * 100
    print(f"  Transformation success rate: {transform_success:.1f}%")
    print(f"  Load success rate: {load_success:.1f}%")
    print(f"  Data quality: {'Good' if len(transformation_result.validation_errors) == 0 else 'Issues detected'}")
    status = "Success" if load_result.success and transform_success > 95 else "Partial success"
    print(f"  Integration status: {status}")


if __name__ == "__main__":
    asyncio.run(main())
