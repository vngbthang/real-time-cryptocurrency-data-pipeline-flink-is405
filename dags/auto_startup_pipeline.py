from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
import time

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 8),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

dag = DAG(
    'auto_startup_pipeline',
    default_args=default_args,
    description='Automatically start crypto pipeline on Airflow startup',
    schedule_interval='@once',  # Run once on startup
    catchup=False,
    tags=['crypto', 'auto-start', 'initialization'],
)

def wait_for_data():
    """Wait for producer to send some data"""
    print("Waiting 60 seconds for producer to send initial data...")
    time.sleep(60)
    print("Initial data wait complete!")

# Task 1: Trigger Spark streaming job
start_spark_job = BashOperator(
    task_id='start_spark_streaming',
    bash_command='airflow dags trigger crypto_streaming_pipeline',
    dag=dag,
)

# Task 2: Wait for data
wait_task = PythonOperator(
    task_id='wait_for_initial_data',
    python_callable=wait_for_data,
    dag=dag,
)

# Task 3: Unpause Gold aggregation DAGs
unpause_hourly = BashOperator(
    task_id='unpause_hourly_aggregation',
    bash_command='airflow dags unpause gold_hourly_aggregation',
    dag=dag,
)

unpause_10min = BashOperator(
    task_id='unpause_10min_aggregation',
    bash_command='airflow dags unpause gold_10min_aggregation',
    dag=dag,
)

# Task 4: Trigger initial Gold aggregation
trigger_hourly = BashOperator(
    task_id='trigger_hourly_aggregation',
    bash_command='airflow dags trigger gold_hourly_aggregation',
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)

trigger_10min = BashOperator(
    task_id='trigger_10min_aggregation',
    bash_command='airflow dags trigger gold_10min_aggregation',
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)

# Define task dependencies
start_spark_job >> wait_task >> [unpause_hourly, unpause_10min]
unpause_hourly >> trigger_hourly
unpause_10min >> trigger_10min
