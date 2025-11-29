#!/bin/bash

echo "Waiting for Airflow to be ready..."
sleep 60

# Trigger Spark streaming pipeline
echo "Triggering Spark streaming pipeline..."
airflow dags unpause crypto_streaming_pipeline
airflow dags trigger crypto_streaming_pipeline

# Unpause Gold aggregation DAGs
echo "Unpausing Gold layer DAGs..."
airflow dags unpause gold_hourly_aggregation
airflow dags unpause gold_10min_aggregation

echo "Airflow initialization complete!"
