#!/bin/bash

# Wait for Kafka to be ready
echo "Waiting for Kafka to be ready..."
sleep 30

# Create topic if not exists
kafka-topics --create --topic crypto_prices \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1 \
  --if-not-exists

echo "Kafka topic 'crypto_prices' created successfully"

# List all topics to verify
kafka-topics --list --bootstrap-server localhost:9092

# Keep container running
tail -f /dev/null
