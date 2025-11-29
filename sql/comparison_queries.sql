-- Query 1: Total Records Comparison
-- Shows total number of records processed by each engine
SELECT 
    'Spark' as engine,
    COUNT(*) as total_records
FROM crypto_prices_realtime
UNION ALL
SELECT 
    'Flink' as engine,
    COUNT(*) as total_records
FROM crypto_prices_flink;
-- Query 2: Average Latency (Last 5 minutes)
-- Measures end-to-end latency from event time to processed time
SELECT 
    'Spark' as engine,
    ROUND(AVG(EXTRACT(EPOCH FROM (processed_at - TO_TIMESTAMP(timestamp))))::numeric, 2) as avg_latency_seconds,
    COUNT(*) as sample_size
FROM crypto_prices_realtime
WHERE processed_at > NOW() - INTERVAL '5 minutes'
UNION ALL
SELECT 
    'Flink' as engine,
    ROUND(AVG(EXTRACT(EPOCH FROM (processed_at - TO_TIMESTAMP(timestamp))))::numeric, 2) as avg_latency_seconds,
    COUNT(*) as sample_size
FROM crypto_prices_flink
WHERE processed_at > NOW() - INTERVAL '5 minutes';

-- Query 3: Latest 5 Records with Latency (Spark)
-- Shows most recent records and their individual latencies
SELECT 
    'Spark' as engine,
    symbol,
    ROUND(price::numeric, 2) as price,
    EXTRACT(EPOCH FROM (processed_at - TO_TIMESTAMP(timestamp)))::integer as latency_seconds,
    processed_at
FROM crypto_prices_realtime
ORDER BY processed_at DESC
LIMIT 5;

-- Query 4: Latest 5 Records with Latency (Flink)
SELECT 
    'Flink' as engine,
    symbol,
    ROUND(price::numeric, 2) as price,
    EXTRACT(EPOCH FROM (processed_at - TO_TIMESTAMP(timestamp)))::integer as latency_seconds,
    processed_at
FROM crypto_prices_flink
ORDER BY processed_at DESC
LIMIT 5;
-- Query 5: Throughput (Records per minute)
-- Calculates processing rate in last 5 minutes
SELECT 
    'Spark' as engine,
    ROUND((COUNT(*)::numeric / EXTRACT(EPOCH FROM (MAX(processed_at) - MIN(processed_at))) * 60), 2) as records_per_minute
FROM crypto_prices_realtime
WHERE processed_at > NOW() - INTERVAL '5 minutes'
UNION ALL
SELECT 
    'Flink' as engine,
    ROUND((COUNT(*)::numeric / EXTRACT(EPOCH FROM (MAX(processed_at) - MIN(processed_at))) * 60), 2) as records_per_minute
FROM crypto_prices_flink
WHERE processed_at > NOW() - INTERVAL '5 minutes';

-- Query 6: Data Freshness
-- Shows how long since last write
SELECT 
    'Spark' as engine,
    EXTRACT(EPOCH FROM (NOW() - MAX(processed_at)))::integer as seconds_since_last_write,
    MAX(processed_at) as last_write_time
FROM crypto_prices_realtime
UNION ALL
SELECT 
    'Flink' as engine,
    EXTRACT(EPOCH FROM (NOW() - MAX(processed_at)))::integer as seconds_since_last_write,
    MAX(processed_at) as last_write_time
FROM crypto_prices_flink;

-- Query 7: Records Distribution by Symbol
SELECT 
    'Spark' as engine,
    symbol,
    COUNT(*) as record_count,
    ROUND(AVG(price)::numeric, 2) as avg_price,
    ROUND(MIN(price)::numeric, 2) as min_price,
    ROUND(MAX(price)::numeric, 2) as max_price
FROM crypto_prices_realtime
GROUP BY symbol
UNION ALL
SELECT 
    'Flink' as engine,
    symbol,
    COUNT(*) as record_count,
    ROUND(AVG(price)::numeric, 2) as avg_price,
    ROUND(MIN(price)::numeric, 2) as min_price,
    ROUND(MAX(price)::numeric, 2) as max_price
FROM crypto_prices_flink
GROUP BY symbol
ORDER BY engine, symbol;

-- Query 8: Latency Percentiles (Last 5 minutes)
-- Shows p50, p95, p99 latencies
WITH spark_latencies AS (
    SELECT 
        EXTRACT(EPOCH FROM (processed_at - TO_TIMESTAMP(timestamp))) as latency
    FROM crypto_prices_realtime
    WHERE processed_at > NOW() - INTERVAL '5 minutes'
),
flink_latencies AS (
    SELECT 
        EXTRACT(EPOCH FROM (processed_at - TO_TIMESTAMP(timestamp))) as latency
    FROM crypto_prices_flink
    WHERE processed_at > NOW() - INTERVAL '5 minutes'
)
SELECT 
    'Spark' as engine,
    ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY latency)::numeric, 2) as p50_latency,
    ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY latency)::numeric, 2) as p95_latency,
    ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY latency)::numeric, 2) as p99_latency
FROM spark_latencies
UNION ALL
SELECT 
    'Flink' as engine,
    ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY latency)::numeric, 2) as p50_latency,
    ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY latency)::numeric, 2) as p95_latency,
    ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY latency)::numeric, 2) as p99_latency
FROM flink_latencies;

-- Query 9: Time Series - Records per Minute (Last 10 minutes)
-- Shows processing rate over time
SELECT 
    'Spark' as engine,
    DATE_TRUNC('minute', processed_at) as minute,
    COUNT(*) as records
FROM crypto_prices_realtime
WHERE processed_at > NOW() - INTERVAL '10 minutes'
GROUP BY DATE_TRUNC('minute', processed_at)
UNION ALL
SELECT 
    'Flink' as engine,
    DATE_TRUNC('minute', processed_at) as minute,
    COUNT(*) as records
FROM crypto_prices_flink
WHERE processed_at > NOW() - INTERVAL '10 minutes'
GROUP BY DATE_TRUNC('minute', processed_at)
ORDER BY minute DESC, engine;

-- Query 10: Summary Statistics
-- Overall comparison summary
WITH spark_stats AS (
    SELECT 
        COUNT(*) as total_records,
        ROUND(AVG(EXTRACT(EPOCH FROM (processed_at - TO_TIMESTAMP(timestamp))))::numeric, 2) as avg_latency,
        ROUND(MIN(EXTRACT(EPOCH FROM (processed_at - TO_TIMESTAMP(timestamp))))::numeric, 2) as min_latency,
        ROUND(MAX(EXTRACT(EPOCH FROM (processed_at - TO_TIMESTAMP(timestamp))))::numeric, 2) as max_latency,
        COUNT(DISTINCT symbol) as unique_symbols
    FROM crypto_prices_realtime
    WHERE processed_at > NOW() - INTERVAL '5 minutes'
),
flink_stats AS (
    SELECT 
        COUNT(*) as total_records,
        ROUND(AVG(EXTRACT(EPOCH FROM (processed_at - TO_TIMESTAMP(timestamp))))::numeric, 2) as avg_latency,
        ROUND(MIN(EXTRACT(EPOCH FROM (processed_at - TO_TIMESTAMP(timestamp))))::numeric, 2) as min_latency,
        ROUND(MAX(EXTRACT(EPOCH FROM (processed_at - TO_TIMESTAMP(timestamp))))::numeric, 2) as max_latency,
        COUNT(DISTINCT symbol) as unique_symbols
    FROM crypto_prices_flink
    WHERE processed_at > NOW() - INTERVAL '5 minutes'
)
SELECT 
    'Spark' as engine,
    total_records,
    avg_latency,
    min_latency,
    max_latency,
    unique_symbols
FROM spark_stats
UNION ALL
SELECT 
    'Flink' as engine,
    total_records,
    avg_latency,
    min_latency,
    max_latency,
    unique_symbols
FROM flink_stats;
