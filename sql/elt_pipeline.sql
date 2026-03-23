-- =============================================================================
-- Reproducible ELT pipeline (PostgreSQL) — Medallion Architecture
-- Order: bronze -> silver -> gold
--
-- Storage design:
--   • Bronze and Silver are UNLOGGED (no WAL) to halve disk I/O.
--   • Silver has a PRIMARY KEY on transaction_id for fast ON CONFLICT
--     deduplication across batches.
--   • Bronze is staging-only and is dropped after silver is populated.
--
-- For the full 398 M-row dataset the Python pipeline (src/silver.py) streams
-- each parquet file through an ephemeral TEMP TABLE and upserts into silver,
-- so bronze and silver are never both fully materialised on disk at once.
--
-- This SQL script is the reproducible equivalent for a single-batch / small
-- dataset load: populate bronze first (e.g. via \copy or COPY FROM STDIN),
-- then execute this script end-to-end.
-- =============================================================================


-- ---------------------------------------------------------------------------
-- 0. Tear-down (safe re-run)
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS gold_channel_share      CASCADE;
DROP TABLE IF EXISTS gold_by_mcc             CASCADE;
DROP TABLE IF EXISTS gold_top_counterparties CASCADE;
DROP TABLE IF EXISTS gold_by_type            CASCADE;
DROP TABLE IF EXISTS gold_by_channel         CASCADE;
DROP TABLE IF EXISTS gold_daily              CASCADE;
DROP TABLE IF EXISTS silver                  CASCADE;
DROP TABLE IF EXISTS bronze                  CASCADE;


-- ---------------------------------------------------------------------------
-- 1. Bronze — raw ingestion schema (UNLOGGED = no WAL overhead)
-- ---------------------------------------------------------------------------
CREATE UNLOGGED TABLE bronze (
    transaction_id          TEXT,
    account_id              TEXT,
    transaction_timestamp   TIMESTAMPTZ,
    mcc_code                BIGINT,
    channel                 TEXT,
    amount                  DOUBLE PRECISION,
    txn_type                TEXT,
    counterparty_id         TEXT,
    loaded_at               TIMESTAMPTZ DEFAULT NOW()
);

-- Populate bronze here for small / sample datasets, e.g.:
--   \copy bronze (transaction_id, account_id, transaction_timestamp,
--                 mcc_code, channel, amount, txn_type, counterparty_id)
--   FROM 'data/raw/part_0001.csv' WITH (FORMAT CSV, HEADER true);
--
-- For the full 398-file dataset use the Python pipeline:
--   uv run python -m src.main


-- ---------------------------------------------------------------------------
-- 2. Silver — cleaned, deduplicated layer (UNLOGGED, PRIMARY KEY)
--
--   The PRIMARY KEY on transaction_id lets the ON CONFLICT clause below
--   resolve duplicates efficiently without a full-table sort or window
--   function materialisation.
-- ---------------------------------------------------------------------------
CREATE UNLOGGED TABLE silver (
    transaction_id        TEXT PRIMARY KEY,
    account_id            TEXT,
    transaction_timestamp TIMESTAMPTZ,
    mcc_code              BIGINT,
    channel               TEXT,
    amount                DOUBLE PRECISION,
    txn_type              TEXT,
    counterparty_id       TEXT
);

-- Populate silver from bronze.
-- ON CONFLICT keeps the row with the later transaction_timestamp
-- (mirrors the ROW_NUMBER() OVER (PARTITION BY … ORDER BY … DESC) logic).
INSERT INTO silver (
    transaction_id,
    account_id,
    transaction_timestamp,
    mcc_code,
    channel,
    amount,
    txn_type,
    counterparty_id
)
SELECT
    transaction_id,
    account_id,
    transaction_timestamp,
    mcc_code,
    channel,
    amount,
    txn_type,
    counterparty_id
FROM bronze
WHERE transaction_id IS NOT NULL
  AND transaction_id <> ''
ON CONFLICT (transaction_id) DO UPDATE
    SET account_id            = EXCLUDED.account_id,
        transaction_timestamp = EXCLUDED.transaction_timestamp,
        mcc_code              = EXCLUDED.mcc_code,
        channel               = EXCLUDED.channel,
        amount                = EXCLUDED.amount,
        txn_type              = EXCLUDED.txn_type,
        counterparty_id       = EXCLUDED.counterparty_id
    WHERE silver.transaction_timestamp IS NULL
       OR EXCLUDED.transaction_timestamp > silver.transaction_timestamp;

-- Bronze is staging-only: drop it now to free disk before building Gold.
DROP TABLE IF EXISTS bronze CASCADE;


-- ---------------------------------------------------------------------------
-- 3. Gold — curated analytics layer (built from silver only)
-- ---------------------------------------------------------------------------

-- 3a. Daily transaction summary
CREATE TABLE gold_daily AS
SELECT
    DATE(transaction_timestamp) AS day,
    COUNT(*)                    AS txn_count,
    COUNT(DISTINCT account_id)  AS unique_accounts,
    SUM(amount)                 AS total_amount,
    AVG(amount)                 AS avg_amount,
    MIN(amount)                 AS min_amount,
    MAX(amount)                 AS max_amount
FROM silver
GROUP BY 1
ORDER BY 1;

-- 3b. Aggregation by channel
CREATE TABLE gold_by_channel AS
SELECT
    channel,
    COUNT(*)    AS txn_count,
    SUM(amount) AS total_amount,
    AVG(amount) AS avg_amount
FROM silver
GROUP BY 1
ORDER BY txn_count DESC;

-- 3c. Aggregation by transaction type
CREATE TABLE gold_by_type AS
SELECT
    txn_type,
    COUNT(*)    AS txn_count,
    SUM(amount) AS total_amount,
    AVG(amount) AS avg_amount
FROM silver
GROUP BY 1;

-- 3d. Top-20 counterparties by total volume
CREATE TABLE gold_top_counterparties AS
SELECT
    counterparty_id,
    COUNT(*)    AS txn_count,
    SUM(amount) AS total_volume
FROM silver
WHERE counterparty_id IS NOT NULL
  AND counterparty_id <> ''
GROUP BY 1
ORDER BY total_volume DESC
LIMIT 20;

-- 3e. Top-20 MCC codes by total amount
CREATE TABLE gold_by_mcc AS
SELECT
    mcc_code,
    COUNT(*)    AS txn_count,
    SUM(amount) AS total_amount
FROM silver
GROUP BY 1
ORDER BY total_amount DESC
LIMIT 20;

-- 3f. Channel share — JOIN-based gold table
--     Demonstrates the required JOIN / aggregation at the Gold layer.
CREATE TABLE gold_channel_share AS
WITH channel_agg AS (
    SELECT
        channel,
        COUNT(*)    AS txn_count,
        SUM(amount) AS total_amount
    FROM silver
    GROUP BY 1
),
totals AS (
    SELECT
        COUNT(*)    AS total_txn,
        SUM(amount) AS total_amount
    FROM silver
)
SELECT
    c.channel,
    c.txn_count,
    c.total_amount,
    c.txn_count::NUMERIC / NULLIF(t.total_txn,    0) AS txn_share,
    c.total_amount        / NULLIF(t.total_amount, 0) AS amount_share
FROM channel_agg c
JOIN totals t ON TRUE
ORDER BY c.txn_count DESC;
