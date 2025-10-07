"""
Basic CAISO connector usage example.

This example demonstrates how to use the CAISO connector to extract
market data from CAISO's OASIS system.
"""

import asyncio
import logging
from datetime import datetime, timedelta, timezone

from connectors.caiso import CAISOConnector, CAISOConnectorConfig


async def main():
    """Main example function."""
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
    
    try:
        print("Starting CAISO connector example...")
        
        # Define extraction parameters
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(hours=24)  # Last 24 hours
        
        print(f"Extracting data from {start_time} to {end_time}")
        
        # Extract data
        extraction_result = await connector.extract(
            start=start_time,
            end=end_time,
            market_run_id="DAM",
            node="TH_SP15_GEN-APND"
        )
        
        print(f"Extraction completed:")
        print(f"  Records extracted: {extraction_result.record_count}")
        print(f"  Metadata: {extraction_result.metadata}")
        
        if extraction_result.data:
            print(f"  Sample record: {extraction_result.data[0]}")
        
        # Transform data
        transformation_result = await connector.transform(extraction_result)
        
        print(f"Transformation completed:")
        print(f"  Records transformed: {transformation_result.record_count}")
        print(f"  Validation errors: {len(transformation_result.validation_errors)}")
        
        if transformation_result.validation_errors:
            print("  Validation errors:")
            for error in transformation_result.validation_errors[:5]:  # Show first 5
                print(f"    - {error}")
        
        if transformation_result.data:
            print(f"  Sample transformed record: {transformation_result.data[0]}")
        
        # Load data (simulate)
        load_result = await connector.load(transformation_result)
        
        print(f"Load completed:")
        print(f"  Records loaded: {load_result.record_count}")
        print(f"  Load metadata: {load_result.metadata}")
        
        print("CAISO connector example completed successfully!")
        
    except Exception as e:
        print(f"Error in CAISO connector example: {e}")
        logging.error(f"CAISO connector example failed: {e}")
        raise
    
    finally:
        # Cleanup
        await connector.cleanup()


if __name__ == "__main__":
    asyncio.run(main())