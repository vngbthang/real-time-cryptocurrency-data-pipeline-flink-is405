from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 8),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'crypto_price_producer',
    default_args=default_args,
    description='Run Coinbase producer script to fetch prices and send to Kafka',
    schedule_interval=None,  # Manual trigger - sau khi trigger sẽ chạy liên tục
    catchup=False,
    tags=['crypto', 'producer', 'kafka'],
)

# Task để chạy producer script
# Lưu ý: Script này sẽ chạy trong vòng lặp vô hạn nên cần chạy background
run_producer = BashOperator(
    task_id='run_coinbase_producer',
    bash_command='python /opt/airflow/dags/coinbase_producer.py',
    dag=dag,
)
