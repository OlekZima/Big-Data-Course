{{
    config(
        materialized='table',
        alias='gold_daily'
    )
}}

SELECT
    DATE(transaction_timestamp) AS day,
    COUNT(*)                     AS txn_count,
    COUNT(DISTINCT account_id)   AS unique_accounts,
    SUM(amount)                  AS total_amount,
    AVG(amount)                  AS avg_amount,
    MIN(amount)                  AS min_amount,
    MAX(amount)                  AS max_amount
FROM {{ source('public', 'silver') }}
GROUP BY 1
ORDER BY 1
