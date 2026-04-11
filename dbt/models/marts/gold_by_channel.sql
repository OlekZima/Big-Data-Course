{{
    config(
        materialized='table',
        alias='gold_by_channel'
    )
}}

SELECT
    channel,
    COUNT(*)    AS txn_count,
    SUM(amount) AS total_amount,
    AVG(amount) AS avg_amount
FROM {{ source('public', 'silver') }}
GROUP BY 1
ORDER BY txn_count DESC
