"""
Gold layer: curated tables ready for analytics.
"""

from utils.db import get_connection


def create_gold_tables():
    # GOLD 1: Daily transactions
    sql_daily = """
    CREATE TABLE IF NOT EXISTS gold_daily AS
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
    """
    # GOLD 2: By channel
    sql_channel = """
    CREATE TABLE IF NOT EXISTS gold_by_channel AS
    SELECT
        channel,
        COUNT(*)          AS txn_count,
        SUM(amount)       AS total_amount,
        AVG(amount)       AS avg_amount
    FROM silver
    GROUP BY 1
    ORDER BY txn_count DESC;
    """
    # GOLD 3: By transaction type (D/C)
    sql_type = """
    CREATE TABLE IF NOT EXISTS gold_by_type AS
    SELECT
        txn_type,
        COUNT(*)    AS txn_count,
        SUM(amount) AS total_amount,
        AVG(amount) AS avg_amount
    FROM silver
    GROUP BY 1;
    """
    # GOLD 4: Top 20 counterparties
    sql_counterparty = """
    CREATE TABLE IF NOT EXISTS gold_top_counterparties AS
    SELECT
        counterparty_id,
        COUNT(*)    AS txn_count,
        SUM(amount) AS total_volume
    FROM silver
    WHERE counterparty_id IS NOT NULL AND counterparty_id != ''
    GROUP BY 1
    ORDER BY total_volume DESC
    LIMIT 20;
    """
    # GOLD 5: MCC code analysis
    sql_mcc = """
    CREATE TABLE IF NOT EXISTS gold_by_mcc AS
    SELECT
        mcc_code,
        COUNT(*)    AS txn_count,
        SUM(amount) AS total_amount
    FROM silver
    GROUP BY 1
    ORDER BY total_amount DESC
    LIMIT 20;
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            for name, sql in [
                ("gold_daily", sql_daily),
                ("gold_by_channel", sql_channel),
                ("gold_by_type", sql_type),
                ("gold_top_counterparties", sql_counterparty),
                ("gold_by_mcc", sql_mcc),
            ]:
                cur.execute(f"DROP TABLE IF EXISTS {name} CASCADE;")
                cur.execute(sql)
                print(f"[GOLD] Created {name}")

    # Final summary
    with get_connection() as conn:
        with conn.cursor() as cur:
            for table in [
                "gold_daily",
                "gold_by_channel",
                "gold_by_type",
                "gold_top_counterparties",
                "gold_by_mcc",
            ]:
                cur.execute(f"SELECT COUNT(*) FROM {table};")
                count = cur.fetchone()[0]
                print(f"[GOLD] {table}: {count:,} rows")


if __name__ == "__main__":
    create_gold_tables()
