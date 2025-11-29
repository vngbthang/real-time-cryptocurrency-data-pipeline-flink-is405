from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Định nghĩa default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 8),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Tạo DAG
dag = DAG(
    'crypto_streaming_pipeline',
    default_args=default_args,
    description='Submit Spark Streaming job to process crypto prices',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['crypto', 'streaming', 'spark'],
)

# Task để submit Spark job
submit_spark_job = BashOperator(
    task_id='submit_spark_streaming_job',
    bash_command='''
    docker exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0 \
        /opt/spark/apps/spark_stream_processor.py
    ''',
    dag=dag,
)