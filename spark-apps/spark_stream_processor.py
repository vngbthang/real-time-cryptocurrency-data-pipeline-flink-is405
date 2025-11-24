from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType

# --- Cấu hình ---
KAFKA_TOPIC = "crypto_prices"
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
SPARK_MASTER_URL = "spark://spark-master:7077"

# Cấu hình PostgreSQL
POSTGRES_URL = "jdbc:postgresql://postgres-db:5432/crypto_data"
POSTGRES_TABLE = "crypto_prices_realtime"
POSTGRES_PROPERTIES = {
    "user": "user",
    "password": "password",
    "driver": "org.postgresql.Driver"
}

def main():
    print("=== Khởi tạo Spark Session ===")
    
    spark = (
        SparkSession.builder
        .appName("CryptoStreamProcessor")
        .master(SPARK_MASTER_URL)
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "org.postgresql:postgresql:42.6.0")
        .getOrCreate()
    )
    
    spark.sparkContext.setLogLevel("WARN")
    print("=== Spark Session đã được tạo ===")

    # 1. Đọc từ Kafka
    print(f"=== Đang đọc stream từ Kafka topic: {KAFKA_TOPIC} ===")
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    # 2. Biến đổi
    json_schema = StructType([
        StructField("timestamp", LongType(), True),
        StructField("symbol", StringType(), True),
        StructField("base", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("price", FloatType(), True),
        StructField("volume_24h", FloatType(), True),
        StructField("source", StringType(), True),
        StructField("iteration", LongType(), True)
    ])
    
    processed_df = (
        kafka_df
        .select(col("value").cast("string").alias("json_value"))
        .select(from_json(col("json_value"), json_schema).alias("data"))
        .select("data.*")
        .withColumn("processed_at", current_timestamp())
    )

    print("=== Schema đã được định nghĩa. Bắt đầu ghi stream ===")

    # 3. Viết vào PostgreSQL
    def write_to_postgres(df, epoch_id):
        try:
            df.write.jdbc(
                url=POSTGRES_URL,
                table=POSTGRES_TABLE,
                mode="append",
                properties=POSTGRES_PROPERTIES
            )
            print(f"✅ Đã ghi batch {epoch_id} vào PostgreSQL thành công ({df.count()} records)")
        except Exception as e:
            print(f"❌ Lỗi khi ghi batch {epoch_id}: {e}")

    query = (
        processed_df.writeStream
        .foreachBatch(write_to_postgres)
        .outputMode("update")
        .trigger(processingTime='15 seconds')
        .option("checkpointLocation", "/opt/spark/apps/checkpoints")
        .start()
    )
    
    print(f"=== Stream đã bắt đầu. Đang chờ dữ liệu từ topic {KAFKA_TOPIC} ===")
    
    query.awaitTermination()

if __name__ == "__main__":
    main()
