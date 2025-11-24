from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 8),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'gold_hourly_aggregation',
    default_args=default_args,
    description='Aggregate Silver data into Gold hourly metrics',
    schedule_interval=timedelta(minutes=10),  # Chạy mỗi 10 phút
    catchup=False,
    tags=['gold', 'aggregation', 'analytics'],
)

def calculate_hourly_metrics(**context):
    """
    Đọc dữ liệu từ Silver layer (crypto_prices_realtime)
    Tính toán metrics theo giờ và ghi vào Gold layer (gold_hourly_metrics)
    """
    
    # Kết nối đến PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id='postgres_crypto')
    
    # SQL để tính toán metrics theo giờ
    aggregation_sql = """
    WITH hourly_data AS (
        SELECT 
            date_trunc('hour', processed_at) as hour_timestamp,
            symbol,
            AVG(price) as avg_price,
            MIN(price) as min_price,
            MAX(price) as max_price,
            SUM(volume_24h) as total_volume,
            AVG(volume_24h) as avg_volume,
            COUNT(*) as record_count
        FROM crypto_prices_realtime
        WHERE processed_at >= NOW() - INTERVAL '2 hours'  -- Chỉ xử lý 2 giờ gần nhất
        GROUP BY date_trunc('hour', processed_at), symbol
    ),
    previous_hour AS (
        SELECT 
            hour_timestamp + INTERVAL '1 hour' as next_hour,
            symbol,
            avg_price as prev_avg_price
        FROM gold_hourly_metrics
        WHERE hour_timestamp >= NOW() - INTERVAL '3 hours'
    )
    INSERT INTO gold_hourly_metrics (
        hour_timestamp,
        symbol,
        avg_price,
        min_price,
        max_price,
        total_volume,
        avg_volume,
        price_change,
        price_change_percent,
        record_count
    )
    SELECT 
        h.hour_timestamp,
        h.symbol,
        h.avg_price,
        h.min_price,
        h.max_price,
        h.total_volume,
        h.avg_volume,
        COALESCE(h.avg_price - p.prev_avg_price, 0) as price_change,
        COALESCE(
            CASE 
                WHEN p.prev_avg_price > 0 
                THEN ((h.avg_price - p.prev_avg_price) / p.prev_avg_price) * 100
                ELSE 0
            END, 
            0
        ) as price_change_percent,
        h.record_count
    FROM hourly_data h
    LEFT JOIN previous_hour p 
        ON h.hour_timestamp = p.next_hour 
        AND h.symbol = p.symbol
    ON CONFLICT (hour_timestamp) 
    DO UPDATE SET
        avg_price = EXCLUDED.avg_price,
        min_price = EXCLUDED.min_price,
        max_price = EXCLUDED.max_price,
        price_change = EXCLUDED.price_change,
        price_change_percent = EXCLUDED.price_change_percent,
        record_count = EXCLUDED.record_count,
        created_at = CURRENT_TIMESTAMP;
    """
    
    try:
        # Thực thi SQL
        pg_hook.run(aggregation_sql)
        
        # Lấy số lượng records đã xử lý
        result = pg_hook.get_first(
            "SELECT COUNT(*) FROM gold_hourly_metrics WHERE created_at >= NOW() - INTERVAL '1 minute';"
        )
        
        count = result[0] if result else 0
        print(f"✅ Đã tổng hợp thành công! {count} hourly metrics đã được cập nhật/tạo mới.")
        
        # Log sample data
        sample = pg_hook.get_records(
            "SELECT * FROM gold_hourly_metrics ORDER BY hour_timestamp DESC LIMIT 3;"
        )
        print(f"📊 Sample data:\n{sample}")
        
        return count
        
    except Exception as e:
        print(f"❌ Lỗi khi tổng hợp dữ liệu: {str(e)}")
        raise

def cleanup_old_data(**context):
    """
    Xóa dữ liệu cũ hơn 7 ngày để tối ưu storage
    """
    pg_hook = PostgresHook(postgres_conn_id='postgres_crypto')
    
    cleanup_sql = """
    DELETE FROM gold_hourly_metrics 
    WHERE hour_timestamp < NOW() - INTERVAL '7 days';
    """
    
    try:
        result = pg_hook.run(cleanup_sql)
        print(f"✅ Đã xóa dữ liệu cũ thành công!")
        return result
    except Exception as e:
        print(f"⚠️ Lỗi khi cleanup: {str(e)}")
        # Không raise error vì cleanup không quan trọng bằng aggregation

# Task 1: Tính toán metrics từ Silver → Gold
aggregate_task = PythonOperator(
    task_id='aggregate_hourly_metrics',
    python_callable=calculate_hourly_metrics,
    provide_context=True,
    dag=dag,
)

# Task 2: Cleanup dữ liệu cũ (optional)
cleanup_task = PythonOperator(
    task_id='cleanup_old_metrics',
    python_callable=cleanup_old_data,
    provide_context=True,
    dag=dag,
)

# Task 3: Validate dữ liệu Gold
validate_sql = """
SELECT 
    COUNT(*) as total_hours,
    MAX(hour_timestamp) as latest_hour,
    SUM(record_count) as total_records_processed
FROM gold_hourly_metrics
WHERE hour_timestamp >= NOW() - INTERVAL '24 hours';
"""

validate_task = PostgresOperator(
    task_id='validate_gold_data',
    postgres_conn_id='postgres_crypto',
    sql=validate_sql,
    dag=dag,
)

# Định nghĩa thứ tự thực thi
aggregate_task >> validate_task >> cleanup_task
