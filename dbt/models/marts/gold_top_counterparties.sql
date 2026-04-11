{{
    config(
        materialized='table',
        alias='gold_top_counterparties'
    )
}}

SELECT
    counterparty_id,
    COUNT(*)    AS txn_count,
    SUM(amount) AS total_volume
FROM {{ source('public', 'silver') }}
WHERE counterparty_id IS NOT NULL
  AND counterparty_id != ''
GROUP BY 1
ORDER BY total_volume DESC
LIMIT 20
