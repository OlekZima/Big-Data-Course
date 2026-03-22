-- Reproducible ELT pipeline (PostgreSQL)
-- Order: bronze -> silver -> gold

DROP TABLE IF EXISTS gold_channel_share CASCADE;
DROP TABLE IF EXISTS gold_by_mcc CASCADE;
DROP TABLE IF EXISTS gold_top_counterparties CASCADE;
DROP TABLE IF EXISTS gold_by_type CASCADE;
DROP TABLE IF EXISTS gold_by_channel CASCADE;
DROP TABLE IF EXISTS gold_daily CASCADE;
DROP TABLE IF EXISTS silver CASCADE;
DROP TABLE IF EXISTS bronze CASCADE;

-- Bronze
CREATE TABLE bronze (
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

-- Silver
CREATE TABLE silver AS
WITH ranked AS (
    SELECT
        transaction_id,
        account_id,
        transaction_timestamp,
        mcc_code,
        channel,
        amount,
        txn_type,
        counterparty_id,
        ROW_NUMBER() OVER (
            PARTITION BY transaction_id
            ORDER BY transaction_timestamp DESC NULLS LAST
        ) AS rn
    FROM bronze
    WHERE transaction_id IS NOT NULL
      AND transaction_id != ''
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
FROM ranked
WHERE rn = 1;

-- Gold
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

CREATE TABLE gold_by_channel AS
SELECT
    channel,
    COUNT(*)          AS txn_count,
    SUM(amount)       AS total_amount,
    AVG(amount)       AS avg_amount
FROM silver
GROUP BY 1
ORDER BY txn_count DESC;

CREATE TABLE gold_by_type AS
SELECT
    txn_type,
    COUNT(*)    AS txn_count,
    SUM(amount) AS total_amount,
    AVG(amount) AS avg_amount
FROM silver
GROUP BY 1;

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

CREATE TABLE gold_by_mcc AS
SELECT
    mcc_code,
    COUNT(*)    AS txn_count,
    SUM(amount) AS total_amount
FROM silver
GROUP BY 1
ORDER BY total_amount DESC
LIMIT 20;

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
