"""
Gold layer: curated tables ready for analytics.
"""

from .sql_queries import (
    create_gold_by_channel_sql,
    create_gold_by_mcc_sql,
    create_gold_by_type_sql,
    create_gold_channel_share_sql,
    create_gold_daily_sql,
    create_gold_top_counterparties_sql,
)
from .utils.db import get_connection
from .utils.logger import get_logger

logger = get_logger(__name__)


def create_gold_tables():
    queries = [
        ("gold_daily", create_gold_daily_sql()),
        ("gold_by_channel", create_gold_by_channel_sql()),
        ("gold_by_type", create_gold_by_type_sql()),
        ("gold_top_counterparties", create_gold_top_counterparties_sql()),
        ("gold_by_mcc", create_gold_by_mcc_sql()),
        ("gold_channel_share", create_gold_channel_share_sql()),
    ]

    with get_connection() as conn:
        with conn.cursor() as cur:
            for name, sql in queries:
                cur.execute(f"DROP TABLE IF EXISTS {name} CASCADE;")
                cur.execute(sql)
                logger.info("[GOLD] Created %s", name)

    # Final summary
    with get_connection() as conn:
        with conn.cursor() as cur:
            for table in [name for name, _ in queries]:
                cur.execute(f"SELECT COUNT(*) FROM {table};")
                count = cur.fetchone()[0]
                logger.info("[GOLD] %s: %s rows", table, f"{count:,}")


if __name__ == "__main__":
    create_gold_tables()
