{{
    config(
        materialized='table',
        alias='gold_channel_share'
    )
}}

WITH channel_agg AS (
    SELECT
        channel,
        COUNT(*)    AS txn_count,
        SUM(amount) AS total_amount
    FROM {{ source('public', 'silver') }}
    GROUP BY 1
),
totals AS (
    SELECT
        COUNT(*)    AS total_txn,
        SUM(amount) AS total_amount
    FROM {{ source('public', 'silver') }}
)
SELECT
    c.channel,
    c.txn_count,
    c.total_amount,
    c.txn_count::numeric / NULLIF(t.total_txn, 0)  AS txn_share,
    c.total_amount / NULLIF(t.total_amount, 0)      AS amount_share
FROM channel_agg c
JOIN totals t ON TRUE
ORDER BY c.txn_count DESC
