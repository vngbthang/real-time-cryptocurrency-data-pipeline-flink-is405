-- Migration script to add volume and additional fields to crypto tables
-- Run this script in the crypto_data database

-- Step 1: Add new columns to crypto_prices_realtime (Silver layer)
ALTER TABLE crypto_prices_realtime 
ADD COLUMN IF NOT EXISTS base VARCHAR(10),
ADD COLUMN IF NOT EXISTS currency VARCHAR(10),
ADD COLUMN IF NOT EXISTS volume_24h FLOAT,
ADD COLUMN IF NOT EXISTS source VARCHAR(20),
ADD COLUMN IF NOT EXISTS iteration BIGINT;

-- Step 2: Add volume columns to gold_hourly_metrics
ALTER TABLE gold_hourly_metrics
ADD COLUMN IF NOT EXISTS total_volume FLOAT,
ADD COLUMN IF NOT EXISTS avg_volume FLOAT;

-- Step 3: Add volume columns to gold_10min_metrics
ALTER TABLE gold_10min_metrics
ADD COLUMN IF NOT EXISTS total_volume FLOAT,
ADD COLUMN IF NOT EXISTS avg_volume FLOAT;

-- Step 4: Create index for better query performance on new fields
CREATE INDEX IF NOT EXISTS idx_crypto_prices_base ON crypto_prices_realtime(base);
CREATE INDEX IF NOT EXISTS idx_crypto_prices_source ON crypto_prices_realtime(source);

-- Verify the changes
SELECT 
    table_name, 
    column_name, 
    data_type 
FROM information_schema.columns 
WHERE table_name IN ('crypto_prices_realtime', 'gold_hourly_metrics', 'gold_10min_metrics')
ORDER BY table_name, ordinal_position;
