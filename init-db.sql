-- Create Silver layer table
CREATE TABLE IF NOT EXISTS crypto_prices_realtime (
    id SERIAL PRIMARY KEY,
    timestamp BIGINT,
    symbol VARCHAR(20),
    base VARCHAR(10),
    currency VARCHAR(10),
    price DOUBLE PRECISION,
    volume_24h DOUBLE PRECISION,
    source VARCHAR(50),
    iteration INTEGER,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create index for better query performance
CREATE INDEX IF NOT EXISTS idx_crypto_prices_symbol ON crypto_prices_realtime(symbol);
CREATE INDEX IF NOT EXISTS idx_crypto_prices_processed_at ON crypto_prices_realtime(processed_at);

-- Create Gold layer hourly metrics table
CREATE TABLE IF NOT EXISTS gold_hourly_metrics (
    id SERIAL PRIMARY KEY,
    hour_timestamp TIMESTAMP,
    symbol VARCHAR(20),
    avg_price DOUBLE PRECISION,
    min_price DOUBLE PRECISION,
    max_price DOUBLE PRECISION,
    opening_price DOUBLE PRECISION,
    closing_price DOUBLE PRECISION,
    price_change DOUBLE PRECISION,
    price_change_percent DOUBLE PRECISION,
    total_volume DOUBLE PRECISION,
    avg_volume DOUBLE PRECISION,
    record_count INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(hour_timestamp, symbol)
);

-- Create index for Gold hourly
CREATE INDEX IF NOT EXISTS idx_gold_hourly_symbol ON gold_hourly_metrics(symbol);
CREATE INDEX IF NOT EXISTS idx_gold_hourly_timestamp ON gold_hourly_metrics(hour_timestamp);

-- Create Gold layer 10-minute metrics table
CREATE TABLE IF NOT EXISTS gold_10min_metrics (
    id SERIAL PRIMARY KEY,
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    symbol VARCHAR(20),
    avg_price DOUBLE PRECISION,
    min_price DOUBLE PRECISION,
    max_price DOUBLE PRECISION,
    price_volatility DOUBLE PRECISION,
    total_volume DOUBLE PRECISION,
    avg_volume DOUBLE PRECISION,
    record_count INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(window_start, symbol)
);

-- Create index for Gold 10-minute
CREATE INDEX IF NOT EXISTS idx_gold_10min_symbol ON gold_10min_metrics(symbol);
CREATE INDEX IF NOT EXISTS idx_gold_10min_window ON gold_10min_metrics(window_start);

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO user;
