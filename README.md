# Apache Flink: Báo cáo và Demo xử lý dữ liệu lớn (So sánh với Apache Spark)

> Báo cáo môn học IS405 – Hệ thống minh họa Flink kết hợp/so sánh với Spark để xử lý dữ liệu streaming quy mô lớn trong thời gian thực.

[![Docker](https://img.shields.io/badge/Docker-Ready-blue)](https://www.docker.com/)
[![Flink](https://img.shields.io/badge/Flink-1.18.0-red)](https://flink.apache.org/)
[![Spark](https://img.shields.io/badge/Spark-3.5.0-orange)](https://spark.apache.org/)
[![Kafka](https://img.shields.io/badge/Kafka-7.3.0-black)](https://kafka.apache.org/)
[![Airflow](https://img.shields.io/badge/Airflow-2.8.1-lightgrey)](https://airflow.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-14-blue)](https://www.postgresql.org/)

---

## 📋 Mục lục

1. Thông tin chung về Apache Flink
2. Đặc trưng, Ưu/Nhược điểm
3. Case study (ứng dụng thực tế)
4. Cấu trúc & Kiến trúc hệ thống demo
5. Cài đặt, kết nối và cách chạy hệ thống
6. So sánh Flink với Spark (kiến trúc & thực nghiệm)
7. Điều chỉnh tham số (tuning)
8. Minh họa vận hành xử lý dữ liệu lớn
9. Kết quả thực nghiệm (số liệu đo)
10. Kết luận

---

## 1. Thông tin chung về Apache Flink

**Apache Flink** là framework và engine phân tán cho xử lý dữ liệu **streaming** và **batch**, nổi bật với:
- Xử lý sự kiện theo thời gian thực, hỗ trợ **Event Time** và **Watermarks**.
- **Exactly-once semantics**, stateful streaming và checkpointing tin cậy.
- Khả năng **scale-out** mạnh mẽ với TaskManagers/JobManager.
- Hỗ trợ API phong phú: DataStream API, Table/SQL, CEP.

Use-cases điển hình: phát hiện gian lận, giám sát hệ thống, phân tích log real-time, recommendation.

---

## 2. Đặc trưng, Ưu/Nhược điểm

**Ưu điểm**
- Độ trễ thấp (low latency) cho streaming thực sự.
- Event-time chuẩn, xử lý out-of-order với watermarks.
- Exactly-once, state backend hiệu quả, checkpoint/restore.
- Tối ưu cho long-running jobs, ổn định khi chạy 24/7.

**Nhược điểm**
- Đường cong học tập (learning curve) cao hơn.
- Hệ sinh thái nhỏ hơn Spark về ML/Batch.
- Tuning cần hiểu rõ parallelism, slot, backpressure.

---

## 3. Case study (tiêu biểu)

- **Alibaba**: xử lý log và giao dịch quy mô lớn, real-time analytics.
- **Netflix/Uber**: giám sát sự kiện hệ thống, phát hiện anomaly real-time.
- **Ververica** (sáng lập Flink): nhiều case ngân hàng/viễn thông.

---

## 4. Cấu trúc & Kiến trúc hệ thống demo

Thư mục chính:

```
coinbase_producer.py           # Producer gửi giá crypto vào Kafka
docker-compose.yml             # Orchestrate toàn bộ services
Dockerfile.producer            # Build producer
Dockerfile.flink               # Build PyFlink app
Dockerfile.spark               # Build Spark streaming app (so sánh)
init-db.sql                    # Tạo schema Silver/Gold
requirements.txt               # Python deps
dags/                          # Airflow DAGs (orchestrate, gold aggregations)
spark-apps/                    # Spark streaming job
flink-apps/                    # Flink streaming job
sql/                           # SQL bổ sung/alter bảng
grafana/                       # Provisioning datasource & dashboards
```

Kiến trúc triển khai:

```
Coinbase API → Kafka → (Flink & Spark) → PostgreSQL (Silver) → Airflow (Gold 10m/hour) → Grafana

Containers: zookeeper, kafka, postgres-db, airflow-{init,webserver,scheduler},
            flink-jobmanager, flink-taskmanager, flink-crypto-processor,
            spark-master, spark-worker, spark-crypto-processor, grafana
```

---

## 5. Cài đặt, kết nối và cách chạy hệ thống

Yêu cầu: Docker Desktop, Windows PowerShell 5.1.

### 5.1 Khởi động hệ thống

```powershell
# Tại thư mục project
docker-compose up -d

# Kiểm tra containers
docker-compose ps
```

### 5.2 Kiểm tra pipeline hoạt động

```powershell
# Kiểm tra Producer
docker logs crypto-producer --tail 20

# Kiểm tra Flink JobManager UI
Start-Process "http://localhost:8082"

# Kiểm tra Spark Master UI
Start-Process "http://localhost:8081"

# Kiểm tra Silver layer (DB)
docker exec postgres-db psql -U user -d crypto_data -c "SELECT COUNT(*) FROM crypto_prices_flink;"
docker exec postgres-db psql -U user -d crypto_data -c "SELECT COUNT(*) FROM crypto_prices_realtime;"

# Kiểm tra Gold layer
docker exec postgres-db psql -U user -d crypto_data -c "SELECT COUNT(*) FROM gold_10min_metrics;"
docker exec postgres-db psql -U user -d crypto_data -c "SELECT COUNT(*) FROM gold_hourly_metrics;"
```

### 5.3 Airflow orchestration

Airflow UI: `http://localhost:8080`
- `crypto_streaming_pipeline`: (nếu dùng) submit job (Spark trước đây). Hiện Spark chạy bằng container riêng.
- `gold_10min_aggregation` và `gold_hourly_aggregation`: tổng hợp Gold layer theo lịch 10 phút.

Tạo Airflow Connection cho PostgreSQL (nếu thiếu):

```powershell
docker exec airflow-webserver airflow connections add 'postgres_crypto' --conn-type 'postgres' --conn-host 'postgres-db' --conn-schema 'crypto_data' --conn-login 'user' --conn-password 'password' --conn-port 5432
```

### 5.4 Grafana dashboard

```powershell
docker-compose up -d grafana
Start-Process "http://localhost:3000"
# Đăng nhập: admin / admin
```

Dashboard: `Real-time Crypto Pipeline` (UID `crypto-pipeline`) hiển thị:
- Giá real-time (Spark/Flink), độ trễ trung bình (5 phút), tổng số records, phân bố theo symbol.
- Gold layer: Avg price theo giờ, % thay đổi giá.

---

## 6. So sánh Flink với Spark

### 6.1 Kiến trúc & ngữ nghĩa
- **Flink**: event-time native, watermarks, stateful streaming, exactly-once mạnh.
- **Spark Structured Streaming**: micro-batch, latency cao hơn, mạnh về batch/SQL/ML.

### 6.2 Triển khai trong hệ thống
- Flink chạy trong container `flink-crypto-processor` (PyFlink), đọc Kafka và ghi PostgreSQL.
- Spark chạy trong container `spark-crypto-processor`, đọc Kafka và ghi PostgreSQL.
- Cả hai cùng đọc `topic: crypto_prices` để so sánh công bằng.

### 6.3 Thực nghiệm đo lường (script `compare_latency.ps1`)

Chạy:
```powershell
& "$PWD\compare_latency.ps1"
```
Các truy vấn: tổng records, latency trung bình/percentiles, throughput, freshness, time-series.

Kết quả tiêu biểu (mẫu):
- Spark avg latency (5m): ~9.0s; Flink avg latency (5m): ~2.1s.
- Throughput: Spark ~40 rec/min; Flink ~25–30 rec/min (tùy thời điểm).
- Percentiles: Flink p95 ~3.5s vs Spark p95 ~14s.

---

## 7. Điều chỉnh tham số (tuning)

### 7.1 Flink
- Parallelism: `flink-apps/flink_stream_processor.py` (set `env.set_parallelism(2)`), `taskmanager.numberOfTaskSlots` trong `docker-compose.yml`.
- Checkpointing: bật trong job PyFlink (nếu cần), cấu hình interval và state backend.
- Kafka source: `properties` như `group.id`, `auto.offset.reset`, `scan.startup.mode`.

### 7.2 Spark
- Worker resources: `SPARK_WORKER_CORES`, `SPARK_WORKER_MEMORY` trong `docker-compose.yml`.
- Batch interval và trigger: trong `spark-apps/spark_stream_processor.py`.
- Kafka options: maxOffsetsPerTrigger, startingOffsets.

### 7.3 PostgreSQL
- Indexes: đã tạo trên timestamp, symbol.
- Connection pool: có thể tinh chỉnh nếu throughput cao.

---

## 8. Minh họa vận hành xử lý dữ liệu lớn

Pipeline minh họa đầy đủ các bước:
1) Producer → Kafka (ingestion liên tục mỗi 10s cho 5 cặp crypto).
2) Flink & Spark → PostgreSQL (Silver): lưu sự kiện kèm `processed_at` để đo latency.
3) Airflow → Gold (10 phút/giờ): tổng hợp avg/min/max/volatility, % thay đổi.
4) Grafana → Dashboard real-time: so sánh Spark vs Flink.

Có thể tăng tải bằng cách:
- Tăng `POLL_INTERVAL_SECONDS` nhỏ lại (ví dụ 5s).
- Tăng số symbol, partitions Kafka, parallelism Flink/Spark.

---

## 9. Kết quả thực nghiệm

Trong phiên chạy hiện tại:
- Silver (Spark): ~1.8k records; Silver (Flink): ~2.0k records.
- Gold 10m: có dữ liệu (10+ bản ghi); Gold hourly: có dữ liệu (5+ bản ghi).
- Latency: Flink thấp hơn đáng kể (p50 ~1.5s vs ~9.5s).

Chi tiết xem tại script `compare_latency.ps1` và dashboard Grafana.

---

## 10. Kết luận

- **Flink** phù hợp cho streaming thời gian thực với yêu cầu độ trễ thấp, exactly-once và event-time chuẩn.
- **Spark** mạnh ở batch, SQL, ML, và hệ sinh thái phong phú; streaming micro-batch có độ trễ cao hơn.
- Hệ thống demo minh họa rõ ràng kiến trúc kết hợp, cách cài đặt/chạy, điều chỉnh tham số, và so sánh thực nghiệm.

---

## Phụ lục: Lệnh nhanh

```powershell
# Khởi động toàn bộ
docker-compose up -d

# Kiểm tra DB nhanh
docker exec postgres-db psql -U user -d crypto_data -c "SELECT COUNT(*) FROM crypto_prices_flink;"

# Chạy script so sánh
& "$PWD\compare_latency.ps1"

# Mở UIs
Start-Process "http://localhost:8082"   # Flink
Start-Process "http://localhost:8081"   # Spark
Start-Process "http://localhost:8080"   # Airflow
Start-Process "http://localhost:3000"   # Grafana
```

## 🏗️ Kiến trúc hệ thống

### Data Flow Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                   CRYPTOCURRENCY DATA PIPELINE                   │
└─────────────────────────────────────────────────────────────────┘

[1] DATA INGESTION
    Coinbase API (REST)
         │
         ├─ GET /products/{symbol}/ticker
         │  └─ Response: {"price": "86746.075", "time": "2024-11-24T..."}
         │
         ▼
    Producer (Python + kafka-python)
         │
         ├─ Poll interval: 10 seconds
         ├─ Symbols: BTC, ETH, SOL, ADA, DOGE
         │
         ▼
    [JSON Message]

[2] MESSAGE BROKER
         │
         ▼
    Apache Kafka (Topic: crypto_prices)
         │
         ├─ Partitions: 3 (for parallelism)
         │
         ├────────────────┬────────────────┐
         │                │                │
         ▼                ▼                ▼
    Partition 0     Partition 1      Partition 2

[3] DUAL STREAM PROCESSING (PARALLEL)

    ┌─────────────────────────────┐  ┌─────────────────────────────┐
    │    SPARK STREAMING          │  │    FLINK STREAMING          │
    ├─────────────────────────────┤  ├─────────────────────────────┤
    │ • Micro-batch (15s trigger) │  │ • Event-driven processing   │
    │ • DataFrame API             │  │ • Table API + SQL DDL       │
    │ • foreachBatch → JDBC       │  │ • JDBC Connector Sink       │
    └─────────────────────────────┘  └─────────────────────────────┘
         │                                    │
         ▼                                    ▼

[4] DATA STORAGE
    PostgreSQL Database (crypto_data)
         │
         ├─ crypto_prices_realtime (Spark writes)
         ├─ crypto_prices_flink (Flink writes)
         ├─ gold_hourly_metrics (aggregated)
         └─ gold_10min_metrics (aggregated)
```

### Infrastructure Components

| Component | Technology | Version | Port | Purpose |
|-----------|------------|---------|------|---------|
| **Message Broker** | Apache Kafka | 7.3.0 | 9092 | Stream data distribution |
| **Coordination** | Zookeeper | 7.3.0 | 2181 | Kafka coordination |
| **Stream Engine 1** | Apache Spark | 3.5.0 | 8081 | Micro-batch processing |
| **Stream Engine 2** | Apache Flink | 1.18.0 | 8082 | True streaming |
| **Database** | PostgreSQL | 14 | 5432 | Data persistence |
| **Orchestration** | Apache Airflow | 2.8.1 | 8080 | Workflow management |
| **Producer** | Python | 3.11 | - | Data ingestion |

---

## 🛠️ Công nghệ sử dụng

### Core Technologies

```yaml
Stream Processing:
  - Apache Spark Structured Streaming 3.5.0
  - Apache Flink DataStream/Table API 1.18.0
  
Message Broker:
  - Apache Kafka 7.3.0
  - Zookeeper 7.3.0
  
Database:
  - PostgreSQL 14
  
Orchestration:
  - Apache Airflow 2.8.1
  
Programming:
  - Python 3.11
  - PyFlink 1.18.0
  - kafka-python 2.0.2
  
Infrastructure:
  - Docker & Docker Compose
  - Linux Containers
```

### Why These Technologies?

**Apache Spark**: Industry standard cho batch + streaming, mature ecosystem  
**Apache Flink**: True streaming với ultra-low latency, exactly-once semantics  
**Kafka**: High-throughput, fault-tolerant message broker  
**PostgreSQL**: ACID-compliant, perfect for analytics  
**Airflow**: Python-native orchestration, easy DAG management  

---

## 🚀 Quick Start - Khởi động nhanh

### Prerequisites

- Docker Desktop (Windows/Mac/Linux)
- 8GB RAM minimum (16GB recommended)
- 20GB disk space
- Internet connection

### Bước 1: Clone Repository

```powershell
git clone https://github.com/vngbthang/real-time-cryptocurrency-data-pipeline-flink-is405.git
cd real-time-cryptocurrency-data-pipeline-flink-is405
```

### Bước 2: Start toàn bộ hệ thống

```powershell
docker-compose up -d
```

**Chờ 2-3 phút** để tất cả services khởi động.

### Bước 3: Verify hệ thống

```powershell
# Check all containers running
docker-compose ps
```

Kết quả mong đợi: **14 containers** với status `Up`:
- ✅ zookeeper
- ✅ kafka
- ✅ postgres-db
- ✅ postgres-airflow-db
- ✅ crypto-producer
- ✅ spark-master
- ✅ spark-worker
- ✅ flink-jobmanager
- ✅ flink-taskmanager
- ✅ flink-crypto-processor
- ✅ airflow-init
- ✅ airflow-webserver
- ✅ airflow-scheduler

### Bước 4: Kiểm tra Producer

```powershell
docker logs crypto-producer --tail 20
```

Kết quả mong đợi:
```
✅ BTC-USD      Price: $   86,865.54
✅ ETH-USD      Price: $    2,833.05
✅ SOL-USD      Price: $      130.35
✅ ADA-USD      Price: $        0.41
✅ DOGE-USD     Price: $        0.15
📊 Summary: 5/5 pairs sent successfully
```

### Bước 5: Verify dữ liệu trong Database

```powershell
docker exec postgres-db psql -U user -d crypto_data -c "SELECT 'Spark' as engine, COUNT(*) FROM crypto_prices_realtime UNION ALL SELECT 'Flink' as engine, COUNT(*) FROM crypto_prices_flink;"
```

Kết quả sau vài phút:
```
 engine | count
--------+-------
 Spark  |   75+
 Flink  |   50+
```

### Bước 6: Chạy Performance Test

```powershell
.\compare_latency.ps1
```

**Kết quả mong đợi:** Flink nhanh hơn Spark **3-5 lần**.

### Bước 7: Truy cập Dashboards

- **Airflow UI:** http://localhost:8080 (admin/admin)
- **Spark Master UI:** http://localhost:8081
- **Flink JobManager UI:** http://localhost:8082

---

## 📊 Apache Flink - Giới thiệu chi tiết

### Kiến trúc Apache Flink

Apache Flink là một **distributed stream processing framework** được thiết kế cho xử lý real-time data với latency cực thấp.

#### Kiến trúc cốt lõi

```
┌────────────────────────────────────────────────────────────────┐
│                    APACHE FLINK ARCHITECTURE                    │
└────────────────────────────────────────────────────────────────┘

[1] CLIENT LAYER
    Flink Application (Python/Java/Scala)
         │
         ├─ DataStream API (imperative)
         ├─ Table API (declarative)
         └─ SQL API (declarative)
         │
         ▼ Submit Job
         
[2] CONTROL PLANE
    JobManager (Master)
         │
         ├─ JobGraph → ExecutionGraph
         ├─ Resource Management
         ├─ Checkpoint Coordination
         └─ Task Scheduling
         │
         ▼ Distribute Tasks
         
[3] DATA PLANE
    TaskManager 1       TaskManager 2       TaskManager 3
    ├─ Task Slot 1      ├─ Task Slot 1      ├─ Task Slot 1
    ├─ Task Slot 2      ├─ Task Slot 2      ├─ Task Slot 2
    └─ Task Slot 3      └─ Task Slot 3      └─ Task Slot 3
         │                   │                   │
         └───────────────────┴───────────────────┘
                             │
                             ▼
[4] STATE MANAGEMENT
    State Backend (RocksDB / Heap)
         │
         ├─ Keyed State (per key)
         ├─ Operator State (per parallel instance)
         └─ Checkpoints (distributed snapshots)
```

#### Core Components

| Component | Vai trò | Số lượng | Docker Service |
|-----------|---------|----------|----------------|
| **JobManager** | Master node, orchestration | 1 | flink-jobmanager |
| **TaskManager** | Worker node, execute tasks | 1+ | flink-taskmanager |
| **Task Slot** | Thread unit for parallelism | N × TaskManager | Configured in env |
| **State Backend** | Persistent storage cho state | 1 (shared) | RocksDB/Heap |

### Ưu điểm và Nhược điểm

#### Ưu điểm

| Ưu điểm | Mô tả | Use Case |
|---------|-------|----------|
| **Low Latency** | Xử lý sub-second latency | Real-time fraud detection, HFT trading |
| **High Throughput** | Millions events/second | IoT data ingestion, log processing |
| **Exactly-Once** | Strong consistency guarantees | Financial transactions, billing systems |
| **Stateful Processing** | Built-in state management | Session analytics, pattern detection |
| **Event Time Processing** | Handle out-of-order events | Time-series analytics, late data handling |
| **Flexible Deployment** | Standalone, YARN, K8s, Mesos | Cloud-native or on-premise |
| **SQL Support** | Table API & SQL for streaming | Business analysts, rapid development |
| **Savepoints** | Version control for streaming apps | A/B testing, rolling updates |

#### Nhược điểm

| Nhược điểm | Mô tả | Mitigation |
|-----------|-------|------------|
| **Steep Learning Curve** | Concepts phức tạp (watermarks, state, checkpoints) | Bắt đầu với Table API trước DataStream API |
| **Memory Intensive** | State backend cần nhiều RAM | Dùng RocksDB cho large state, tune memory configs |
| **Operational Complexity** | Cần monitoring checkpoint lag, backpressure | Dùng Flink Dashboard + Prometheus metrics |
| **Limited ML Support** | Không có ML library như Spark MLlib | Tích hợp với TensorFlow, PyTorch riêng |
| **Smaller Ecosystem** | Ít connectors hơn Spark | Community đang phát triển nhanh |
| **Debugging Challenges** | Distributed debugging khó | Dùng local mode + extensive logging |

---

## ⚖️ So sánh Apache Spark vs Apache Flink

### Kiến trúc xử lý

| Tiêu chí | Apache Spark Structured Streaming | Apache Flink |
|----------|-----------------------------------|--------------|
| **Processing Model** | Micro-batch (15 giây/batch) | True streaming (event-by-event) |
| **Core Abstraction** | RDD → DataFrame/Dataset | DataStream → Table |
| **State Management** | External state stores (HDFS, S3) | Built-in managed state (RocksDB) |
| **Latency** | Seconds (batch interval) | Milliseconds (event-driven) |
| **Throughput** | Excellent for large batches | Excellent for continuous streams |
| **Memory Model** | In-memory caching for speed | Streaming pipelined execution |
| **Fault Tolerance** | RDD lineage + checkpointing | Distributed snapshots (Chandy-Lamport) |

### API Comparison

**Spark Structured Streaming:**
```python
# Declarative API với DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col

spark = SparkSession.builder.appName("CryptoStream").getOrCreate()

# Read stream
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "crypto_prices") \
    .load()

# Transform (batch-like operations)
crypto_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Write stream với trigger interval
query = crypto_df.writeStream \
    .outputMode("complete") \
    .trigger(processingTime="15 seconds") \
    .format("console") \
    .start()
```

**Flink Table API + SQL:**
```python
# DDL-style table creation
from pyflink.table import StreamTableEnvironment

table_env = StreamTableEnvironment.create(env)

# Kafka source
table_env.execute_sql("""
    CREATE TABLE crypto_source (
        symbol STRING,
        price DOUBLE,
        `timestamp` BIGINT,
        WATERMARK FOR event_time AS TO_TIMESTAMP_LTZ(`timestamp`, 3)
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'crypto_prices',
        'properties.bootstrap.servers' = 'kafka:9092',
        'format' = 'json',
        'scan.startup.mode' = 'latest-offset'
    )
""")

# JDBC sink
table_env.execute_sql("""
    CREATE TABLE crypto_sink (
        symbol STRING,
        price DOUBLE,
        `user` STRING,
        `timestamp` BIGINT
    ) WITH (
        'connector' = 'jdbc',
        'url' = 'jdbc:postgresql://postgres-db:5432/crypto_data',
        'table-name' = 'crypto_prices_flink',
        'username' = 'user',
        'password' = 'password'
    )
""")

# Streaming query
table_env.execute_sql("INSERT INTO crypto_sink SELECT * FROM crypto_source")
```

### Performance Comparison

| Metric | Spark (Micro-batch 15s) | Flink (True Streaming) |
|--------|-------------------------|------------------------|
| **Latency** | 8-9 giây | 1-3 giây |
| **Throughput** | ~27 records/minute | ~26 records/minute |
| **Memory Usage** | 2-4 GB (executor heap) | 1-3 GB (task manager) |
| **CPU Usage** | Spiky (batch processing) | Smooth (continuous) |
| **Exactly-Once** | ✅ Với foreachBatch | ✅ Native support |
| **Late Data Handling** | ⚠️ Limited watermark support | ✅ Advanced watermark strategies |

### Time Semantics

**Spark:**
```python
# Processing time (khi data đến Spark)
df.writeStream \
    .trigger(processingTime="15 seconds") \
    .start()

# Event time (limited support)
df.withWatermark("timestamp", "10 minutes")
```

**Flink:**
```python
# Event time với watermark strategy
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.time import Duration

strategy = WatermarkStrategy \
    .for_bounded_out_of_orderness(Duration.of_seconds(5)) \
    .with_timestamp_assigner(lambda event, ts: event['timestamp'])

stream.assign_timestamps_and_watermarks(strategy)
```

### State Management

| Feature | Spark | Flink |
|---------|-------|-------|
| **State Store** | External (HDFS/S3) | Embedded (RocksDB/Memory) |
| **State Size** | Limited by batch size | Unlimited (RocksDB disk) |
| **State Access** | Batch-based | Continuous access |
| **Checkpointing** | Incremental (Delta files) | Asynchronous barriers |
| **Recovery Time** | Minutes (batch replay) | Seconds (state restore) |

### Windowing Capabilities

**Spark (Limited):**
```python
# Fixed windows only
df.groupBy(
    window("timestamp", "5 minutes")
).count()
```

**Flink (Comprehensive):**
```python
# Tumbling Window
stream.key_by(...).window(TumblingEventTimeWindows.of(Time.minutes(5)))

# Sliding Window
stream.key_by(...).window(SlidingEventTimeWindows.of(
    Time.minutes(10),  # size
    Time.minutes(5)    # slide
))

# Session Window (activity gap-based)
stream.key_by(...).window(EventTimeSessionWindows.with_gap(Time.minutes(30)))

# Global Window với custom triggers
stream.key_by(...).window(GlobalWindows.create()).trigger(...)
```

---

## 🔧 Điều chỉnh tham số Flink

### Cấu hình trong docker-compose.yml

```yaml
flink-jobmanager:
  image: flink:1.18.0-scala_2.12-java11
  environment:
    - |
      FLINK_PROPERTIES=
      # === PARALLELISM & SLOTS ===
      taskmanager.numberOfTaskSlots: 4           # Số task slots mỗi TaskManager
      parallelism.default: 2                     # Parallelism mặc định
      
      # === MEMORY CONFIGURATION ===
      taskmanager.memory.process.size: 2048m     # Tổng memory cho TaskManager
      taskmanager.memory.flink.size: 1536m       # Flink managed memory
      
      # === CHECKPOINT SETTINGS ===
      execution.checkpointing.interval: 60000    # Checkpoint mỗi 60 giây
      execution.checkpointing.mode: EXACTLY_ONCE # At-least-once hoặc exactly-once
      
      # === STATE BACKEND ===
      state.backend: rocksdb                     # rocksdb hoặc filesystem
      state.checkpoints.dir: file:///tmp/flink-checkpoints
```

### Performance Tuning Parameters

**1. Parallelism (Độ song song)**
```python
env = StreamExecutionEnvironment.get_execution_environment()

# Set global parallelism
env.set_parallelism(4)

# Set per-operator parallelism
stream.map(my_function).set_parallelism(8)
```

**Nguyên tắc:** `parallelism = số TaskManager × số slots per TaskManager`  
**Demo này:** 3 Kafka partitions → parallelism=2 hoặc 3

**2. Checkpointing (Fault Tolerance)**
```python
# Enable checkpointing
env.enable_checkpointing(60000)  # 60 seconds

# Checkpoint configuration
checkpoint_config = env.get_checkpoint_config()
checkpoint_config.set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)
checkpoint_config.set_checkpoint_timeout(600000)  # 10 min timeout
```

**3. State Backend Selection**

| State Backend | Use Case | Max Size | Performance |
|---------------|----------|----------|-------------|
| **HashMap** | Small state (<100MB) | Limited by heap | Very fast |
| **RocksDB** | Large state (GBs-TBs) | Disk-bounded | Moderate (disk I/O) |

**4. Buffer Timeout (Latency Tuning)**
```python
env.set_buffer_timeout(100)  # milliseconds
```

| Buffer Timeout | Latency | Throughput | Use Case |
|----------------|---------|------------|----------|
| 0ms | Lowest | Lowest | Ultra-low latency apps |
| 100ms | Low | High | Balanced (recommended) |
| -1 (disabled) | Highest | Highest | Batch-like processing |

**5. Kafka Consumer Configuration**
```python
table_env.execute_sql("""
    CREATE TABLE crypto_source (...) WITH (
        'connector' = 'kafka',
        'properties.group.id' = 'flink-crypto-consumer',
        'scan.startup.mode' = 'latest-offset',
        'properties.fetch.min.bytes' = '1024',
        'properties.max.partition.fetch.bytes' = '1048576'
    )
""")
```

**6. JDBC Sink Tuning**
```python
table_env.execute_sql("""
    CREATE TABLE crypto_sink (...) WITH (
        'connector' = 'jdbc',
        'sink.buffer-flush.max-rows' = '100',         # Batch size
        'sink.buffer-flush.interval' = '1s',          # Flush interval
        'sink.max-retries' = '3',                     # Retry on failure
        'sink.parallelism' = '2'                      # Writer parallelism
    )
""")
```

---

## 📈 Performance Verification - Bằng chứng Flink nhanh hơn

### Chạy Performance Test

```powershell
.\compare_latency.ps1
```

Script này chạy 4 tests để đo và so sánh hiệu suất giữa Spark và Flink.

### Test 1: Average Latency

**Đo latency trung bình trong 5 phút gần đây:**

```sql
SELECT 
    engine,
    AVG(processed_at_timestamp - producer_timestamp) as avg_latency_sec,
    COUNT(*) as sample_size
FROM (Spark table UNION Flink table)
WHERE processed_at > NOW() - INTERVAL '5 minutes';
```

**Kết quả:**
```
 engine | avg_latency_sec | sample_size 
--------+-----------------+-------------
 Spark  |            8.83 |         120
 Flink  |            2.23 |         125
```

**Phân tích:**
- ✅ **Flink nhanh hơn 3.96x** (8.83s vs 2.23s)
- Spark: 8-9 giây latency do micro-batch processing
- Flink: 2-3 giây latency nhờ event-driven architecture

### Test 2: Latest Records Latency

**5 records mới nhất từ mỗi engine:**

**Spark:**
```
  symbol  | latency_sec | db_time  
----------+-------------+----------
 DOGE-USD |           7 | 09:09:00
 ADA-USD  |           7 | 09:09:00
 SOL-USD  |           7 | 09:09:00
 ETH-USD  |           7 | 09:09:00
 BTC-USD  |           7 | 09:09:00
```

**Flink:**
```
  symbol  | latency_sec | db_time  
----------+-------------+----------
 DOGE-USD |           4 | 09:09:08
 ADA-USD  |           3 | 09:09:07
 SOL-USD  |           2 | 09:09:06
 ETH-USD  |           1 | 09:09:05
 BTC-USD  |           1 | 09:09:05
```

**Phân tích:**
- Spark: Tất cả records **cùng latency (7s)** vì batch processing
- Flink: Latency **khác nhau (1-4s)** vì xử lý từng event
- ✅ **Flink nhanh hơn 5-7x**

### Test 3: Throughput Comparison

```
 engine |     records_per_min     
--------+-------------------------
 Spark  | 26.67
 Flink  | 26.11
```

**Phân tích:** ✅ **Throughput tương đương** (~26 records/min)

### Test 4: Data Freshness

```
 engine | time_since_last_write 
--------+-----------------------
 Spark  | 00:00:15.77
 Flink  | 00:00:06.91
```

**Phân tích:**
- Spark: Data cũ hơn **15.77 giây**
- Flink: Data chỉ cũ **6.91 giây**
- ✅ **Flink data mới hơn 2.3x**

### Giải thích tại sao Flink nhanh hơn

#### Spark Micro-batch Processing

```
Timeline:
00:00  Producer sends → Kafka
00:00  ├─ Message arrives in Kafka
00:00  ├─ Spark: Waiting for trigger (15s interval)
00:15  └─ Trigger! Read all messages from last 15s
00:16      ├─ Parse JSON
00:17      ├─ Transform data
00:18      └─ Write batch to PostgreSQL
       
Total Latency: 15-18 seconds
```

**Nguyên nhân chậm:**
- ⏱️ **Trigger Interval = 15 giây:** Phải đợi đủ thời gian mới xử lý
- 📦 **Batch Processing:** Tất cả messages trong 15s được xử lý cùng lúc
- **Minimum Latency = Trigger Interval**

#### Flink Event-Driven Processing

```
Timeline:
00:00  Producer sends → Kafka
00:00  ├─ Message arrives in Kafka
00:01  ├─ Flink reads event immediately
00:01  ├─ Parse JSON (in-flight)
00:02  ├─ Transform data (in-flight)
00:02  └─ Write to PostgreSQL immediately

Total Latency: 1-3 seconds
```

**Nguyên nhân nhanh:**
- ⚡ **Event-Driven:** Xử lý ngay khi message đến
- 🔄 **Pipelined Execution:** Parse → Transform → Write song song
- 💨 **No Waiting:** Không có trigger interval

### Bảng tóm tắt Performance

| Metric | Spark | Flink | Winner |
|--------|-------|-------|--------|
| **Avg Latency** | 8.83s | 2.23s | ✅ Flink (3.96x) |
| **Min Latency** | 15s | 1s | ✅ Flink (15x) |
| **Throughput** | 26.67 rec/min | 26.11 rec/min | ⚖️ Equal |
| **Data Freshness** | 15.77s old | 6.91s old | ✅ Flink (2.3x) |

---

## 🎯 Kết luận & Lựa chọn

### Khi nào dùng Flink?

✅ **Real-time dashboards:** Cần update < 5 giây  
✅ **Fraud detection:** Phát hiện gian lận ngay lập tức  
✅ **Live monitoring:** Giám sát hệ thống real-time  
✅ **Trading systems:** High-frequency trading  
✅ **IoT streaming:** Sensor data processing  
✅ **Alerting systems:** Gửi alert trong vài giây  

### Khi nào dùng Spark?

✅ **ETL pipelines:** Batch + streaming trong cùng code  
✅ **Data warehousing:** Load data mỗi 15-30 phút  
✅ **Machine Learning:** Training models trên streaming data  
✅ **Report generation:** Tạo báo cáo định kỳ  
✅ **Large batch jobs:** Xử lý terabytes data  

### Bảng lựa chọn

| Tiêu chí | Spark | Flink | Chọn gì? |
|----------|-------|-------|----------|
| **Latency requirement** | 10-30s OK | < 5s cần | Flink cho real-time |
| **Data volume** | Terabytes | Gigabytes | Spark cho big batch |
| **Team experience** | Spark ecosystem | Flink learning curve | Spark dễ hơn |
| **Use case** | Analytics, ML | Monitoring, alerting | Depends |
| **Cost** | Lower (batch efficient) | Higher (always running) | Spark rẻ hơn |

---

## 🛑 Stop System

```powershell
# Stop all containers
docker-compose down

# Stop and remove volumes (clean slate)
docker-compose down -v
```

---

## 📝 Kết luận tổng quan

**Apache Spark Structured Streaming** và **Apache Flink** đều là công cụ mạnh mẽ cho xử lý streaming:

- **Spark**: Phù hợp cho batch + streaming, latency 8-15 giây, dễ học nếu đã biết Spark ecosystem
- **Flink**: Latency thấp 1-3 giây, event-driven, phức tạp hơn nhưng mạnh mẽ cho real-time analytics

**Kết quả thực tế từ demo này:**
- Producer gửi 5 crypto pairs mỗi 10 giây
- Spark xử lý theo batch 15 giây → **latency 8.83s**
- Flink xử lý real-time từng event → **latency 2.23s**
- Cả hai đều ghi vào PostgreSQL để so sánh side-by-side

**Bằng chứng cụ thể:** Chạy `.\compare_latency.ps1` để xem Flink nhanh hơn Spark **3.96 lần**.

**Lựa chọn phụ thuộc vào:** Yêu cầu latency, data volume, team experience, và budget.

