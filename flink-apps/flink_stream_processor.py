"""
Apache Flink Stream Processor for Cryptocurrency Data
Writes to PostgreSQL database for comparison with Spark Streaming
"""

import logging
import sys

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

try:
    from pyflink.datastream import StreamExecutionEnvironment
    from pyflink.table import StreamTableEnvironment, EnvironmentSettings
    
    KAFKA_TOPIC = "crypto_prices"
    KAFKA_SERVERS = "kafka:9092"
    POSTGRES_URL = "jdbc:postgresql://postgres-db:5432/crypto_data"
    
    def main():
        logger.info("=== Starting Flink Stream Processor ===")
        
        env = StreamExecutionEnvironment.get_execution_environment()
        env.set_parallelism(1)
        
        settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
        table_env = StreamTableEnvironment.create(env, environment_settings=settings)
        
        # Create Kafka source
        source_ddl = f"""
            CREATE TABLE kafka_source (
                `timestamp` BIGINT,
                `symbol` STRING,
                `base` STRING,
                `currency` STRING,
                `price` DOUBLE,
                `volume_24h` DOUBLE,
                `source` STRING,
                `iteration` BIGINT,
                `proc_time` AS PROCTIME()
            ) WITH (
                'connector' = 'kafka',
                'topic' = '{KAFKA_TOPIC}',
                'properties.bootstrap.servers' = '{KAFKA_SERVERS}',
                'properties.group.id' = 'flink-consumer',
                'scan.startup.mode' = 'latest-offset',
                'format' = 'json'
            )
        """
        table_env.execute_sql(source_ddl)
        logger.info("Kafka source created")
        
        # Create PostgreSQL sink (match database schema without id - it's auto-generated)
        sink_ddl = f"""
            CREATE TABLE postgres_sink (
                `timestamp` BIGINT,
                `symbol` STRING,
                `base` STRING,
                `currency` STRING,
                `price` DOUBLE,
                `volume_24h` DOUBLE,
                `source` STRING,
                `iteration` BIGINT
            ) WITH (
                'connector' = 'jdbc',
                'url' = '{POSTGRES_URL}',
                'table-name' = 'crypto_prices_flink',
                'username' = 'user',
                'password' = 'password',
                'driver' = 'org.postgresql.Driver'
            )
        """
        table_env.execute_sql(sink_ddl)
        logger.info("PostgreSQL sink created")
        
        # Insert data (streaming query - will run indefinitely)
        insert_sql = """
            INSERT INTO postgres_sink
            SELECT `timestamp`, symbol, base, currency, price, volume_24h, source, iteration
            FROM kafka_source
            WHERE price IS NOT NULL AND price > 0
        """
        
        logger.info("Starting data processing...")
        # This blocks and runs the streaming job
        table_result = table_env.execute_sql(insert_sql)
        logger.info("Flink job submitted successfully")
        
        # Wait for job to complete (it won't - it's a streaming job)
        table_result.wait()
        
    if __name__ == "__main__":
        main()
        
except Exception as e:
    logger.error(f"Error: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

