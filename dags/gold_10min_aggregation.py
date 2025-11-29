from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 8),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'gold_10min_aggregation',
    default_args=default_args,
    description='Real-time 10-minute aggregation from Silver to Gold',
    schedule_interval=timedelta(minutes=10),  # Chạy mỗi 10 phút
    catchup=False,
    tags=['gold', 'real-time', 'analytics'],
)

def aggregate_10min_metrics(**context):
    """
    Tổng hợp metrics 10 phút từ Silver layer
    """
    pg_hook = PostgresHook(postgres_conn_id='postgres_crypto')
    
    aggregation_sql = """
    INSERT INTO gold_10min_metrics (
        window_start,
        symbol,
        avg_price,
        min_price,
        max_price,
        total_volume,
        avg_volume,
        price_volatility,
        record_count
    )
    SELECT 
        date_trunc('hour', processed_at) + 
            (FLOOR(EXTRACT(MINUTE FROM processed_at) / 10) * 10) * INTERVAL '1 minute' as window_start,
        symbol,
        AVG(price) as avg_price,
        MIN(price) as min_price,
        MAX(price) as max_price,
        SUM(volume_24h) as total_volume,
        AVG(volume_24h) as avg_volume,
        STDDEV(price) as price_volatility,
        COUNT(*) as record_count
    FROM crypto_prices_realtime
    WHERE processed_at >= NOW() - INTERVAL '30 minutes'
    GROUP BY 
        date_trunc('hour', processed_at) + 
            (FLOOR(EXTRACT(MINUTE FROM processed_at) / 10) * 10) * INTERVAL '1 minute',
        symbol
    ON CONFLICT (window_start, symbol) 
    DO UPDATE SET
        avg_price = EXCLUDED.avg_price,
        min_price = EXCLUDED.min_price,
        max_price = EXCLUDED.max_price,
        total_volume = EXCLUDED.total_volume,
        avg_volume = EXCLUDED.avg_volume,
        price_volatility = EXCLUDED.price_volatility,
        record_count = EXCLUDED.record_count,
        created_at = CURRENT_TIMESTAMP;
    """
    
    try:
        pg_hook.run(aggregation_sql)
        
        # Lấy kết quả mới nhất
        result = pg_hook.get_records("""
            SELECT 
                window_start,
                symbol,
                ROUND(avg_price::numeric, 2) as avg_price,
                ROUND(min_price::numeric, 2) as min_price,
                ROUND(max_price::numeric, 2) as max_price,
                ROUND((max_price - min_price)::numeric, 2) as range,
                ROUND(price_volatility::numeric, 2) as volatility,
                record_count
            FROM gold_10min_metrics
            ORDER BY window_start DESC
            LIMIT 3;
        """)
        
        print(f"✅ Đã tổng hợp 10-minute metrics thành công!")
        print(f"📊 Latest windows:")
        for row in result:
            print(f"   {row[0]} | {row[1]} | Avg: ${row[2]:,.2f} | Range: ${row[5]:,.2f} | Records: {row[7]}")
        
        return len(result)
        
    except Exception as e:
        print(f"❌ Lỗi: {str(e)}")
        raise

def generate_summary_stats(**context):
    """
    Tạo summary statistics cho dashboard
    """
    pg_hook = PostgresHook(postgres_conn_id='postgres_crypto')
    
    summary_sql = """
    SELECT 
        symbol,
        COUNT(*) as total_windows,
        ROUND(AVG(avg_price)::numeric, 2) as overall_avg_price,
        ROUND(MIN(min_price)::numeric, 2) as lowest_price,
        ROUND(MAX(max_price)::numeric, 2) as highest_price,
        ROUND(AVG(price_volatility)::numeric, 4) as avg_volatility,
        SUM(record_count) as total_records
    FROM gold_10min_metrics
    WHERE window_start >= NOW() - INTERVAL '24 hours'
    GROUP BY symbol;
    """
    
    try:
        result = pg_hook.get_records(summary_sql)
        
        print(f"\n📈 24-Hour Summary Statistics:")
        print(f"{'Symbol':<10} | {'Windows':<8} | {'Avg Price':<12} | {'Low':<12} | {'High':<12} | {'Volatility':<12} | {'Records':<10}")
        print("-" * 90)
        
        for row in result:
            print(f"{row[0]:<10} | {row[1]:<8} | ${row[2]:>10,.2f} | ${row[3]:>10,.2f} | ${row[4]:>10,.2f} | {row[5]:>10,.4f} | {row[6]:<10}")
        
        return result
        
    except Exception as e:
        print(f"⚠️ Warning: {str(e)}")
        return []

# Tasks
aggregate_task = PythonOperator(
    task_id='aggregate_10min_window',
    python_callable=aggregate_10min_metrics,
    provide_context=True,
    dag=dag,
)

summary_task = PythonOperator(
    task_id='generate_summary',
    python_callable=generate_summary_stats,
    provide_context=True,
    dag=dag,
)

# Task dependencies
aggregate_task >> summary_task
