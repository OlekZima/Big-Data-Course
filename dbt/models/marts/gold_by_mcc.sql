{{
    config(
        materialized='table',
        alias='gold_by_mcc'
    )
}}

SELECT
    mcc_code,
    COUNT(*)    AS txn_count,
    SUM(amount) AS total_amount
FROM {{ source('public', 'silver') }}
GROUP BY 1
ORDER BY total_amount DESC
LIMIT 20
