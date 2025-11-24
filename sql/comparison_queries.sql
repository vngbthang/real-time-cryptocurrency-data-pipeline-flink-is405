-- Script SQL để so sánh hiệu suất giữa Spark Streaming và Apache Flink
-- File: sql/comparison_queries.sql

-- ============================================
-- 1. SO SÁNH TỔNG QUAN
-- ============================================

-- Tổng số records được xử lý bởi mỗi engine
SELECT 
    'Spark Streaming' as engine,
    COUNT(*) as total_records,
    MIN(processed_at) as first_record,
    MAX(processed_at) as last_record,
    EXTRACT(EPOCH FROM (MAX(processed_at) - MIN(processed_at))) as duration_seconds
FROM crypto_prices_realtime

UNION ALL

SELECT 
    'Apache Flink' as engine,
    COUNT(*) as total_records,
    MIN(processed_at) as first_record,
    MAX(processed_at) as last_record,
    EXTRACT(EPOCH FROM (MAX(processed_at) - MIN(processed_at))) as duration_seconds
FROM crypto_prices_flink;


-- ============================================
-- 2. SO SÁNH ĐỘ TRỄ (LATENCY)
-- ============================================

-- Độ trễ trung bình (thời gian từ lúc event xảy ra đến lúc được xử lý)
WITH spark_latency AS (
    SELECT 
        'Spark Streaming' as engine,
        EXTRACT(EPOCH FROM (processed_at - TO_TIMESTAMP(timestamp/1000))) as latency_seconds
    FROM crypto_prices_realtime
    WHERE processed_at > NOW() - INTERVAL '10 minutes'
),
flink_latency AS (
    SELECT 
        'Apache Flink' as engine,
        EXTRACT(EPOCH FROM (processed_at - TO_TIMESTAMP(timestamp/1000))) as latency_seconds
    FROM crypto_prices_flink
    WHERE processed_at > NOW() - INTERVAL '10 minutes'
)
SELECT 
    engine,
    COUNT(*) as sample_size,
    ROUND(AVG(latency_seconds)::numeric, 2) as avg_latency_sec,
    ROUND(MIN(latency_seconds)::numeric, 2) as min_latency_sec,
    ROUND(MAX(latency_seconds)::numeric, 2) as max_latency_sec,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY latency_seconds)::numeric, 2) as median_latency_sec,
    ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY latency_seconds)::numeric, 2) as p95_latency_sec,
    ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY latency_seconds)::numeric, 2) as p99_latency_sec
FROM (
    SELECT * FROM spark_latency
    UNION ALL
    SELECT * FROM flink_latency
) combined
GROUP BY engine
ORDER BY avg_latency_sec;


-- ============================================
-- 3. SO SÁNH THÔNG LƯỢNG (THROUGHPUT)
-- ============================================

-- Records được xử lý mỗi phút (5 phút gần nhất)
WITH time_windows AS (
    SELECT generate_series(
        DATE_TRUNC('minute', NOW() - INTERVAL '5 minutes'),
        DATE_TRUNC('minute', NOW()),
        '1 minute'::interval
    ) as window_start
),
spark_throughput AS (
    SELECT 
        DATE_TRUNC('minute', processed_at) as minute,
        COUNT(*) as record_count
    FROM crypto_prices_realtime
    WHERE processed_at > NOW() - INTERVAL '5 minutes'
    GROUP BY DATE_TRUNC('minute', processed_at)
),
flink_throughput AS (
    SELECT 
        DATE_TRUNC('minute', processed_at) as minute,
        COUNT(*) as record_count
    FROM crypto_prices_flink
    WHERE processed_at > NOW() - INTERVAL '5 minutes'
    GROUP BY DATE_TRUNC('minute', processed_at)
)
SELECT 
    tw.window_start as minute,
    COALESCE(st.record_count, 0) as spark_records,
    COALESCE(ft.record_count, 0) as flink_records,
    COALESCE(ft.record_count, 0) - COALESCE(st.record_count, 0) as difference
FROM time_windows tw
LEFT JOIN spark_throughput st ON tw.window_start = st.minute
LEFT JOIN flink_throughput ft ON tw.window_start = ft.minute
ORDER BY tw.window_start DESC;


-- ============================================
-- 4. SO SÁNH DỮ LIỆU THEO SYMBOL
-- ============================================

-- Phân bố dữ liệu theo symbol (cryptocurrency)
SELECT 
    'Spark Streaming' as engine,
    symbol,
    COUNT(*) as record_count,
    ROUND(AVG(price)::numeric, 2) as avg_price,
    ROUND(MIN(price)::numeric, 2) as min_price,
    ROUND(MAX(price)::numeric, 2) as max_price
FROM crypto_prices_realtime
WHERE processed_at > NOW() - INTERVAL '1 hour'
GROUP BY symbol

UNION ALL

SELECT 
    'Apache Flink' as engine,
    symbol,
    COUNT(*) as record_count,
    ROUND(AVG(price)::numeric, 2) as avg_price,
    ROUND(MIN(price)::numeric, 2) as min_price,
    ROUND(MAX(price)::numeric, 2) as max_price
FROM crypto_prices_flink
WHERE processed_at > NOW() - INTERVAL '1 hour'
GROUP BY symbol

ORDER BY symbol, engine;


-- ============================================
-- 5. REAL-TIME MONITORING
-- ============================================

-- Live view: Records trong 1 phút gần nhất
SELECT 
    'Spark Streaming' as engine,
    COUNT(*) as records_last_minute,
    COUNT(*) * 60 as estimated_records_per_hour,
    MAX(processed_at) as latest_record_time
FROM crypto_prices_realtime
WHERE processed_at > NOW() - INTERVAL '1 minute'

UNION ALL

SELECT 
    'Apache Flink' as engine,
    COUNT(*) as records_last_minute,
    COUNT(*) * 60 as estimated_records_per_hour,
    MAX(processed_at) as latest_record_time
FROM crypto_prices_flink
WHERE processed_at > NOW() - INTERVAL '1 minute';


-- ============================================
-- 6. DATA QUALITY CHECK
-- ============================================

-- Kiểm tra dữ liệu NULL hoặc không hợp lệ
SELECT 
    'Spark Streaming' as engine,
    COUNT(*) as total_records,
    COUNT(*) FILTER (WHERE price IS NULL OR price <= 0) as invalid_price,
    COUNT(*) FILTER (WHERE symbol IS NULL) as null_symbol,
    COUNT(*) FILTER (WHERE timestamp IS NULL) as null_timestamp
FROM crypto_prices_realtime

UNION ALL

SELECT 
    'Apache Flink' as engine,
    COUNT(*) as total_records,
    COUNT(*) FILTER (WHERE price IS NULL OR price <= 0) as invalid_price,
    COUNT(*) FILTER (WHERE symbol IS NULL) as null_symbol,
    COUNT(*) FILTER (WHERE timestamp IS NULL) as null_timestamp
FROM crypto_prices_flink;


-- ============================================
-- 7. DUPLICATE DETECTION
-- ============================================

-- Phát hiện duplicate records (cùng timestamp + symbol)
WITH spark_dupes AS (
    SELECT 
        timestamp, 
        symbol, 
        COUNT(*) as dup_count
    FROM crypto_prices_realtime
    GROUP BY timestamp, symbol
    HAVING COUNT(*) > 1
),
flink_dupes AS (
    SELECT 
        timestamp, 
        symbol, 
        COUNT(*) as dup_count
    FROM crypto_prices_flink
    GROUP BY timestamp, symbol
    HAVING COUNT(*) > 1
)
SELECT 
    'Spark Streaming' as engine,
    COUNT(*) as duplicate_groups,
    SUM(dup_count) as total_duplicates
FROM spark_dupes

UNION ALL

SELECT 
    'Apache Flink' as engine,
    COUNT(*) as duplicate_groups,
    SUM(dup_count) as total_duplicates
FROM flink_dupes;


-- ============================================
-- 8. TEMPORAL CONSISTENCY
-- ============================================

-- Kiểm tra xem có records nào bị xử lý ra ngoài thứ tự thời gian không
WITH spark_order AS (
    SELECT 
        timestamp,
        processed_at,
        LAG(timestamp) OVER (ORDER BY processed_at) as prev_timestamp
    FROM crypto_prices_realtime
    WHERE processed_at > NOW() - INTERVAL '10 minutes'
),
flink_order AS (
    SELECT 
        timestamp,
        processed_at,
        LAG(timestamp) OVER (ORDER BY processed_at) as prev_timestamp
    FROM crypto_prices_flink
    WHERE processed_at > NOW() - INTERVAL '10 minutes'
)
SELECT 
    'Spark Streaming' as engine,
    COUNT(*) FILTER (WHERE timestamp < prev_timestamp) as out_of_order_count,
    COUNT(*) as total_records,
    ROUND(100.0 * COUNT(*) FILTER (WHERE timestamp < prev_timestamp) / COUNT(*), 2) as out_of_order_percent
FROM spark_order

UNION ALL

SELECT 
    'Apache Flink' as engine,
    COUNT(*) FILTER (WHERE timestamp < prev_timestamp) as out_of_order_count,
    COUNT(*) as total_records,
    ROUND(100.0 * COUNT(*) FILTER (WHERE timestamp < prev_timestamp) / COUNT(*), 2) as out_of_order_percent
FROM flink_order;


-- ============================================
-- 9. PERFORMANCE SUMMARY REPORT
-- ============================================

-- Tổng hợp toàn bộ metrics để so sánh
WITH metrics AS (
    SELECT 
        'Spark Streaming' as engine,
        COUNT(*) as total_records,
        ROUND(AVG(EXTRACT(EPOCH FROM (processed_at - TO_TIMESTAMP(timestamp/1000))))::numeric, 2) as avg_latency_sec,
        COUNT(DISTINCT symbol) as unique_symbols,
        MAX(processed_at) as last_update
    FROM crypto_prices_realtime
    WHERE processed_at > NOW() - INTERVAL '1 hour'
    
    UNION ALL
    
    SELECT 
        'Apache Flink' as engine,
        COUNT(*) as total_records,
        ROUND(AVG(EXTRACT(EPOCH FROM (processed_at - TO_TIMESTAMP(timestamp/1000))))::numeric, 2) as avg_latency_sec,
        COUNT(DISTINCT symbol) as unique_symbols,
        MAX(processed_at) as last_update
    FROM crypto_prices_flink
    WHERE processed_at > NOW() - INTERVAL '1 hour'
)
SELECT 
    *,
    ROUND((total_records::float / 3600) * 1, 2) as records_per_second
FROM metrics
ORDER BY avg_latency_sec;
