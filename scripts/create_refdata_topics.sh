#!/bin/bash
# Create Kafka topics for reference data pipeline

KAFKA_BOOTSTRAP=${KAFKA_BOOTSTRAP:-"localhost:9092"}

echo "Creating Kafka topics for reference data pipeline..."

# Create ingestion topic
kafka-topics --bootstrap-server $KAFKA_BOOTSTRAP \
  --create \
  --topic ingestion.instruments.raw.v1 \
  --partitions 3 \
  --replication-factor 1 \
  --config cleanup.policy=delete \
  --config retention.ms=604800000 \
  --config segment.ms=3600000

# Create normalized topic
kafka-topics --bootstrap-server $KAFKA_BOOTSTRAP \
  --create \
  --topic normalized.instruments.updated.v1 \
  --partitions 3 \
  --replication-factor 1 \
  --config cleanup.policy=delete \
  --config retention.ms=604800000 \
  --config segment.ms=3600000

# Create served topic
kafka-topics --bootstrap-server $KAFKA_BOOTSTRAP \
  --create \
  --topic served.reference.instruments.v1 \
  --partitions 3 \
  --replication-factor 1 \
  --config cleanup.policy=delete \
  --config retention.ms=604800000 \
  --config segment.ms=3600000

echo "Topics created successfully!"
echo "Listing topics:"
kafka-topics --bootstrap-server $KAFKA_BOOTSTRAP --list | grep -E "(ingestion|normalized|served)"
