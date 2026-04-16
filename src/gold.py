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
from .utils.constants import (
    GOLD_BY_CHANNEL_TABLE,
    GOLD_BY_MCC_TABLE,
    GOLD_BY_TYPE_TABLE,
    GOLD_CHANNEL_SHARE_TABLE,
    GOLD_DAILY_TABLE,
    GOLD_TOP_COUNTERPARTIES_TABLE,
)
from .utils.db import get_connection
from .utils.logger import get_logger

logger = get_logger(__name__)


def create_gold_tables():
    queries = [
        (GOLD_DAILY_TABLE, create_gold_daily_sql()),
        (GOLD_BY_CHANNEL_TABLE, create_gold_by_channel_sql()),
        (GOLD_BY_TYPE_TABLE, create_gold_by_type_sql()),
        (GOLD_TOP_COUNTERPARTIES_TABLE, create_gold_top_counterparties_sql()),
        (GOLD_BY_MCC_TABLE, create_gold_by_mcc_sql()),
        (GOLD_CHANNEL_SHARE_TABLE, create_gold_channel_share_sql()),
    ]

    with get_connection() as conn:
        with conn.cursor() as cur:
            for name, sql in queries:
                cur.execute(f"DROP TABLE IF EXISTS {name} CASCADE;")
                cur.execute(sql)
                logger.info("[GOLD] Created %s", name)

    with get_connection() as conn:
        with conn.cursor() as cur:
            for table in [name for name, _ in queries]:
                cur.execute(f"SELECT COUNT(*) FROM {table};")
                count = cur.fetchone()[0]
                logger.info("[GOLD] %s: %s rows", table, f"{count:,}")


if __name__ == "__main__":
    create_gold_tables()
