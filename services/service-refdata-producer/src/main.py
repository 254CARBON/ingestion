#!/usr/bin/env python3
"""
254Carbon Reference Data Producer Service

Produces sample reference instrument data to Kafka for testing the data pipeline.
"""

import asyncio
import json
import logging
import os
import sys
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Any

from confluent_kafka import Producer


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class RefDataProducer:
    """Reference data producer service."""

    def __init__(self, kafka_bootstrap_servers: str = "localhost:9092"):
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.producer = Producer({
            'bootstrap.servers': kafka_bootstrap_servers,
            'acks': 'all',
            'retries': 3,
            'max.in.flight.requests.per.connection': 1,
        })
        self.topic = "ingestion.instruments.raw.v1"

    def _delivery_callback(self, err, msg):
        """Delivery callback for Kafka messages."""
        if err:
            logger.error(f'Message delivery failed: {err}')
        else:
            logger.debug(f'Message delivered to {msg.topic()} [{msg.partition()}]')

    async def produce_instruments(self, instruments: List[Dict[str, Any]]) -> None:
        """Produce instrument data to Kafka."""
        for instrument_data in instruments:
            # Create event envelope
            event = {
                "event_id": str(uuid.uuid4()),
                "event_version": 1,
                "connector_id": "refdata-producer-001",
                "raw_payload": json.dumps(instrument_data),
                "source_system": "sample-data",
                "instrument_count": 1,
                "ingested_at": int(datetime.now(timezone.utc).timestamp() * 1000),
                "trace_id": str(uuid.uuid4()),
                "tenant_id": "default",
                "retry_count": 0,
                "error_message": None
            }

            # Produce to Kafka
            self.producer.produce(
                self.topic,
                key=instrument_data["instrument_id"],
                value=json.dumps(event),
                callback=self._delivery_callback
            )

        # Wait for all messages to be delivered
        self.producer.flush()

        logger.info(f"Produced {len(instruments)} instrument events to {self.topic}")

    async def generate_sample_instruments(self) -> List[Dict[str, Any]]:
        """Generate sample instrument data."""
        instruments = [
            {
                "instrument_id": "AAPL-US-EQUITY",
                "symbol": "AAPL",
                "name": "Apple Inc.",
                "exchange": "NASDAQ",
                "asset_class": "equity",
                "currency": "USD",
                "is_active": True,
                "sector": "Technology",
                "country": "US",
                "tick_size": 0.01
            },
            {
                "instrument_id": "MSFT-US-EQUITY",
                "symbol": "MSFT",
                "name": "Microsoft Corporation",
                "exchange": "NASDAQ",
                "asset_class": "equity",
                "currency": "USD",
                "is_active": True,
                "sector": "Technology",
                "country": "US",
                "tick_size": 0.01
            },
            {
                "instrument_id": "TSLA-US-EQUITY",
                "symbol": "TSLA",
                "name": "Tesla Inc.",
                "exchange": "NASDAQ",
                "asset_class": "equity",
                "currency": "USD",
                "is_active": True,
                "sector": "Consumer Discretionary",
                "country": "US",
                "tick_size": 0.01
            },
            {
                "instrument_id": "GOOGL-US-EQUITY",
                "symbol": "GOOGL",
                "name": "Alphabet Inc.",
                "exchange": "NASDAQ",
                "asset_class": "equity",
                "currency": "USD",
                "is_active": True,
                "sector": "Technology",
                "country": "US",
                "tick_size": 0.01
            },
            {
                "instrument_id": "AMZN-US-EQUITY",
                "symbol": "AMZN",
                "name": "Amazon.com Inc.",
                "exchange": "NASDAQ",
                "asset_class": "equity",
                "currency": "USD",
                "is_active": True,
                "sector": "Consumer Discretionary",
                "country": "US",
                "tick_size": 0.01
            },
            {
                "instrument_id": "NVDA-US-EQUITY",
                "symbol": "NVDA",
                "name": "NVIDIA Corporation",
                "exchange": "NASDAQ",
                "asset_class": "equity",
                "currency": "USD",
                "is_active": True,
                "sector": "Technology",
                "country": "US",
                "tick_size": 0.01
            },
            {
                "instrument_id": "META-US-EQUITY",
                "symbol": "META",
                "name": "Meta Platforms Inc.",
                "exchange": "NASDAQ",
                "asset_class": "equity",
                "currency": "USD",
                "is_active": True,
                "sector": "Technology",
                "country": "US",
                "tick_size": 0.01
            },
            {
                "instrument_id": "NFLX-US-EQUITY",
                "symbol": "NFLX",
                "name": "Netflix Inc.",
                "exchange": "NASDAQ",
                "asset_class": "equity",
                "currency": "USD",
                "is_active": True,
                "sector": "Communication Services",
                "country": "US",
                "tick_size": 0.01
            }
        ]

        return instruments

    async def run(self) -> None:
        """Main run method."""
        logger.info("Starting reference data producer...")
        
        try:
            # Generate sample instruments
            instruments = await self.generate_sample_instruments()
            
            # Produce to Kafka
            await self.produce_instruments(instruments)
            
            logger.info("Reference data producer completed successfully")
            
        except Exception as e:
            logger.error(f"Error in reference data producer: {e}")
            raise


async def main() -> None:
    """Main entry point."""
    kafka_bootstrap = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
    
    producer = RefDataProducer(kafka_bootstrap)
    await producer.run()


if __name__ == "__main__":
    asyncio.run(main())
