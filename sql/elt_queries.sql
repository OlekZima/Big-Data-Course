-- name: create_bronze_table
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

-- name: create_silver_table
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

-- name: create_gold_daily
CREATE TABLE gold_daily AS
SELECT
    DATE(transaction_timestamp) AS day,
    COUNT(*)                     AS txn_count,
    COUNT(DISTINCT account_id)   AS unique_accounts,
    SUM(amount)                  AS total_amount,
    AVG(amount)                  AS avg_amount,
    MIN(amount)                  AS min_amount,
    MAX(amount)                  AS max_amount
FROM silver
GROUP BY 1
ORDER BY 1;

-- name: create_gold_by_channel
CREATE TABLE gold_by_channel AS
SELECT
    channel,
    COUNT(*)          AS txn_count,
    SUM(amount)       AS total_amount,
    AVG(amount)       AS avg_amount
FROM silver
GROUP BY 1
ORDER BY txn_count DESC;

-- name: create_gold_by_type
CREATE TABLE gold_by_type AS
SELECT
    txn_type,
    COUNT(*)    AS txn_count,
    SUM(amount) AS total_amount,
    AVG(amount) AS avg_amount
FROM silver
GROUP BY 1;

-- name: create_gold_top_counterparties
CREATE TABLE gold_top_counterparties AS
SELECT
    counterparty_id,
    COUNT(*)    AS txn_count,
    SUM(amount) AS total_volume
FROM silver
WHERE counterparty_id IS NOT NULL AND counterparty_id != ''
GROUP BY 1
ORDER BY total_volume DESC
LIMIT 20;

-- name: create_gold_by_mcc
CREATE TABLE gold_by_mcc AS
SELECT
    mcc_code,
    COUNT(*)    AS txn_count,
    SUM(amount) AS total_amount
FROM silver
GROUP BY 1
ORDER BY total_amount DESC
LIMIT 20;

-- name: create_gold_channel_share
CREATE TABLE gold_channel_share AS
WITH channel_agg AS (
    SELECT channel, COUNT(*) AS txn_count, SUM(amount) AS total_amount
    FROM silver
    GROUP BY 1
),
totals AS (
    SELECT COUNT(*) AS total_txn, SUM(amount) AS total_amount
    FROM silver
)
SELECT
    c.channel,
    c.txn_count,
    c.total_amount,
    c.txn_count::numeric / NULLIF(t.total_txn, 0)   AS txn_share,
    c.total_amount / NULLIF(t.total_amount, 0)     AS amount_share
FROM channel_agg c
JOIN totals t ON TRUE
ORDER BY c.txn_count DESC;
