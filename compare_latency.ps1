#!/usr/bin/env pwsh
# Latency Comparison Script: Spark vs Flink

Write-Host "`n================================================" -ForegroundColor Cyan
Write-Host "  LATENCY COMPARISON: SPARK vs FLINK" -ForegroundColor Yellow
Write-Host "================================================`n" -ForegroundColor Cyan

Write-Host "Test 1: Average Latency (5 phut gan day)`n" -ForegroundColor Green

docker exec postgres-db psql -U user -d crypto_data -c @"
SELECT 
    'Spark' as engine,
    ROUND(AVG(EXTRACT(EPOCH FROM processed_at)::BIGINT - timestamp)::numeric, 2) as avg_latency_sec,
    COUNT(*) as sample_size
FROM crypto_prices_realtime
WHERE processed_at > NOW() - INTERVAL '5 minutes'
UNION ALL
SELECT 
    'Flink' as engine,
    ROUND(AVG(EXTRACT(EPOCH FROM processed_at)::BIGINT - timestamp)::numeric, 2),
    COUNT(*)
FROM crypto_prices_flink
WHERE processed_at > NOW() - INTERVAL '5 minutes';
"@

Write-Host "`nTest 2: Latest 5 Records - Latency Chi Tiet`n" -ForegroundColor Green
Write-Host "--- SPARK ---" -ForegroundColor Cyan
docker exec postgres-db psql -U user -d crypto_data -c @"
SELECT symbol, 
       EXTRACT(EPOCH FROM processed_at)::BIGINT - timestamp as latency_sec,
       TO_CHAR(processed_at, 'HH24:MI:SS') as db_time
FROM crypto_prices_realtime 
ORDER BY processed_at DESC LIMIT 5;
"@

Write-Host "`n--- FLINK ---" -ForegroundColor Cyan
docker exec postgres-db psql -U user -d crypto_data -c @"
SELECT symbol, 
       EXTRACT(EPOCH FROM processed_at)::BIGINT - timestamp as latency_sec,
       TO_CHAR(processed_at, 'HH24:MI:SS') as db_time
FROM crypto_prices_flink 
ORDER BY processed_at DESC LIMIT 5;
"@

Write-Host "`nTest 3: Throughput (Records per Minute)`n" -ForegroundColor Green

docker exec postgres-db psql -U user -d crypto_data -c @"
SELECT 
    'Spark' as engine,
    COUNT(*) / NULLIF(EXTRACT(EPOCH FROM (MAX(processed_at) - MIN(processed_at))), 0) * 60 as records_per_min
FROM crypto_prices_realtime
WHERE processed_at > NOW() - INTERVAL '5 minutes'
UNION ALL
SELECT 
    'Flink',
    COUNT(*) / NULLIF(EXTRACT(EPOCH FROM (MAX(processed_at) - MIN(processed_at))), 0) * 60
FROM crypto_prices_flink
WHERE processed_at > NOW() - INTERVAL '5 minutes';
"@

Write-Host "`nTest 4: Data Freshness`n" -ForegroundColor Green

docker exec postgres-db psql -U user -d crypto_data -c @"
SELECT 
    'Spark' as engine,
    NOW() - MAX(processed_at) as time_since_last_write
FROM crypto_prices_realtime
UNION ALL
SELECT 
    'Flink',
    NOW() - MAX(processed_at)
FROM crypto_prices_flink;
"@

Write-Host "`n================================================" -ForegroundColor Cyan
Write-Host "  KET LUAN" -ForegroundColor Yellow
Write-Host "================================================" -ForegroundColor Cyan
Write-Host "Flink NHANH HON neu:" -ForegroundColor Green
Write-Host "  - Latency Flink < 5 giay" -ForegroundColor White
Write-Host "  - Latency Spark >= 15 giay (do batch interval)" -ForegroundColor White
Write-Host "  - Flink co throughput tuong duong hoac cao hon" -ForegroundColor White
Write-Host "`nLy do Spark cham:" -ForegroundColor Yellow
Write-Host "  - Micro-batch: Doi 15 giay moi xu ly 1 batch" -ForegroundColor White
Write-Host "  - Minimum latency = trigger interval" -ForegroundColor White
Write-Host "`nLy do Flink nhanh:" -ForegroundColor Yellow
Write-Host "  - True streaming: Xu ly ngay khi nhan event" -ForegroundColor White
Write-Host "  - Event-driven pipeline" -ForegroundColor White
Write-Host "================================================`n" -ForegroundColor Cyan
