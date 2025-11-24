# Real-Time Cryptocurrency Pipeline - Apache Spark vs Apache Flink

## Giới thiệu
Dự án so sánh hiệu suất xử lý dữ liệu streaming giữa **Apache Spark Structured Streaming** và **Apache Flink DataStream/Table API** trên pipeline thu thập giá cryptocurrency real-time từ Coinbase.

**Kiến trúc hệ thống:**
```
Coinbase API → Producer → Kafka → [Spark Streaming + Flink] → PostgreSQL
```

**5 cặp tiền mã hóa được theo dõi:**
- BTC-USD, ETH-USD, SOL-USD, ADA-USD, DOGE-USD

---

## Prerequisites
- Docker Desktop đã cài đặt và chạy
- 8GB RAM khả dụng
- Internet connection (để lấy dữ liệu từ Coinbase API)

---

## Quick Start - Khởi động hệ thống

### Bước 1: Start toàn bộ hệ thống
```powershell
docker-compose up -d
```

### Bước 2: Đợi services khởi động (2-3 phút)
```powershell
docker-compose ps
```
Đảm bảo tất cả 14 containers có trạng thái `Up`.

### Bước 3: Kiểm tra Producer đang gửi dữ liệu
```powershell
docker logs crypto-producer --tail 20
```
Kết quả mong đợi:
```
✅ Successfully sent 5/5 cryptocurrency pairs to Kafka
```

### Bước 4: Xác minh dữ liệu vào Kafka
```powershell
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic crypto_prices --from-beginning --max-messages 5
```

---

## System Check - Kiểm tra hệ thống

### Check Spark Streaming logs
```powershell
docker logs spark-streaming-consumer --tail 50
```
Tìm các dòng:
```
Batch: X
+----------+------------+
|symbol    |price       |
+----------+------------+
|BTC-USD   |86769.94    |
```

### Check Flink logs
```powershell
docker logs flink-crypto-processor --tail 50
```
Tìm các dòng:
```
[FLINK] BTC-USD: $86746.075
```

### Check tất cả containers
```powershell
docker-compose ps
```
Tất cả phải có trạng thái `Up`.

---

## Database Verification - Kiểm tra dữ liệu trong Database

### Kiểm tra số lượng records từ cả 2 engines
```powershell
docker exec -it postgres-db psql -U postgres -d crypto_db -c "SELECT 'Spark' as engine, COUNT(*) FROM crypto_prices_realtime UNION ALL SELECT 'Flink' as engine, COUNT(*) FROM crypto_prices_flink;"
```

Kết quả hiện tại (sau vài phút):
```
 engine | count
--------+-------
 Spark  |   75+
 Flink  |   50+
```

### Xem dữ liệu mới nhất từ Spark
```powershell
docker exec -it postgres-db psql -U postgres -d crypto_db -c "SELECT symbol, price, \"user\", timestamp FROM crypto_prices_realtime ORDER BY timestamp DESC LIMIT 10;"
```

### Xem dữ liệu mới nhất từ Flink
```powershell
docker exec -it postgres-db psql -U postgres -d crypto_db -c "SELECT symbol, price, \"user\", timestamp FROM crypto_prices_flink ORDER BY timestamp DESC LIMIT 10;"
```

### Xem thống kê theo symbol
```powershell
docker exec -it postgres-db psql -U postgres -d crypto_db -c "SELECT 'Spark' as engine, symbol, COUNT(*) as record_count, AVG(price) as avg_price FROM crypto_prices_realtime GROUP BY symbol UNION ALL SELECT 'Flink' as engine, symbol, COUNT(*) as record_count, AVG(price) as avg_price FROM crypto_prices_flink GROUP BY symbol ORDER BY engine, symbol;"
```

---

## So sánh Apache Spark vs Apache Flink

### 1. Kiến trúc xử lý

| Tiêu chí | Apache Spark Structured Streaming | Apache Flink |
|----------|-----------------------------------|--------------|
| **Processing Model** | Micro-batch (15 giây/batch) | Event-driven streaming (thời gian thực) |
| **API sử dụng** | DataFrame/DataSet API | DataStream API + Table API |
| **Latency** | 15-30 giây (do batch interval) | < 1 giây (xử lý từng event) |
| **Throughput** | Tốt cho batch lớn | Tốt cho event-by-event |

### 2. Code implementation

**Spark (spark-apps/spark_stream_processor.py):**
```python
# Đọc từ Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "crypto_prices") \
    .load()

# Parse JSON và chọn cột
crypto_df = df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json("json", schema).alias("data")) \
    .select("data.*")

# Ghi vào PostgreSQL mỗi 15 giây
query = crypto_df.writeStream \
    .trigger(processingTime='15 seconds') \
    .foreachBatch(write_to_postgres) \
    .start()
```

**Flink (flink-apps/flink_stream_processor.py):**
```python
# Table API - DDL cho Kafka source
table_env.execute_sql("""
    CREATE TABLE crypto_source (
        symbol STRING,
        price DOUBLE,
        `user` STRING,
        `timestamp` BIGINT
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'crypto_prices',
        'properties.bootstrap.servers' = 'kafka:9092',
        'format' = 'json'
    )
""")

# DDL cho JDBC sink
table_env.execute_sql("""
    CREATE TABLE crypto_sink (
        symbol STRING,
        price DOUBLE,
        `user` STRING,
        `timestamp` BIGINT
    ) WITH (
        'connector' = 'jdbc',
        'url' = 'jdbc:postgresql://postgres-db:5432/crypto_db',
        'table-name' = 'crypto_prices_flink',
        'username' = 'postgres',
        'password' = 'postgres'
    )
""")

# Chạy streaming query
table_env.execute_sql("INSERT INTO crypto_sink SELECT * FROM crypto_source")
```

### 3. Hiệu suất quan sát được

Sau 5 phút chạy:

**Spark Streaming:**
- ✅ ~75+ records (5 symbols × 15 records mỗi 15 giây)
- ✅ Batch size ổn định
- ✅ Không có data loss
- ⚠️ Latency cao hơn (15-30 giây)

**Flink:**
- ✅ ~50+ records (event-driven)
- ✅ Latency thấp (< 1 giây)
- ✅ Xử lý thời gian thực
- ⚠️ Cần nhiều memory hơn cho state management

### 4. Trường hợp sử dụng phù hợp

**Chọn Spark khi:**
- ✅ Cần xử lý batch + streaming trong cùng codebase
- ✅ Đã có kinh nghiệm với Spark ecosystem
- ✅ Latency 10-30 giây là chấp nhận được
- ✅ Cần tích hợp với Spark SQL, MLlib

**Chọn Flink khi:**
- ✅ Cần latency < 1 giây
- ✅ Event-driven architecture
- ✅ Complex event processing (CEP)
- ✅ Exactly-once semantics quan trọng

---

## Dashboard URLs

Sau khi hệ thống chạy, truy cập các UI:

- **Spark Master UI:** http://localhost:8080
- **Spark Application UI:** http://localhost:4040
- **Flink JobManager UI:** http://localhost:8081
- **Kafka Manager (Conduktor):** http://localhost:8000
- **Airflow UI:** http://localhost:8085 (user: `airflow`, password: `airflow`)

---

## Troubleshooting

### Producer không gửi dữ liệu
```powershell
# Restart producer
docker-compose restart crypto-producer

# Check logs
docker logs crypto-producer --tail 50
```

### Spark không ghi vào database
```powershell
# Check PostgreSQL connection
docker exec -it postgres-db psql -U postgres -d crypto_db -c "\dt"

# Restart Spark consumer
docker-compose restart spark-streaming-consumer
```

### Flink job failed
```powershell
# Check JobManager logs
docker logs flink-jobmanager --tail 100

# Check TaskManager logs
docker logs flink-taskmanager --tail 100

# Restart Flink services
docker-compose restart flink-jobmanager flink-taskmanager flink-crypto-processor
```

### Database connection errors
```powershell
# Verify database is running
docker exec -it postgres-db psql -U postgres -c "SELECT version();"

# Check tables exist
docker exec -it postgres-db psql -U postgres -d crypto_db -c "\dt"
```

### Kafka topic issues
```powershell
# List topics
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list

# Describe topic
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic crypto_prices

# Recreate topic if needed
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic crypto_prices
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --create --topic crypto_prices --partitions 3 --replication-factor 1
```

---

## Stop System

```powershell
# Stop all containers
docker-compose down

# Stop and remove volumes (clean slate)
docker-compose down -v
```

---

## Kết luận

**Apache Spark Structured Streaming** và **Apache Flink** đều là công cụ mạnh mẽ cho xử lý streaming:

- **Spark**: Phù hợp cho batch + streaming, latency chấp nhận được, dễ học nếu đã biết Spark
- **Flink**: Latency thấp, event-driven, phức tạp hơn nhưng mạnh mẽ cho real-time analytics

Trong demo này:
- Producer gửi 5 crypto pairs mỗi 10 giây
- Spark xử lý theo batch 15 giây
- Flink xử lý real-time từng event
- Cả hai đều ghi vào PostgreSQL để so sánh

**Lựa chọn phụ thuộc vào yêu cầu latency và kinh nghiệm team.**

