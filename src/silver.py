"""
Silver layer: clean data and handle key data quality issues.

This version keeps profiling non-blocking by default by avoiding expensive
full-table DISTINCT counts unless explicitly requested.
"""

from __future__ import annotations

from .sql_queries import create_silver_table_sql
from .utils.db import get_connection
from .utils.logger import get_logger

logger = get_logger(__name__)


def _estimate_row_count(table_name: str) -> int | None:
    """
    Return an estimated row count from PostgreSQL statistics.

    Uses pg_class.reltuples and does not force a full table scan.
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT CASE
                         WHEN c.reltuples < 0 THEN NULL
                         ELSE c.reltuples::bigint
                       END AS est_rows
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = 'public'
                  AND c.relname = %s;
                """,
                (table_name,),
            )
            row = cur.fetchone()
            return row[0] if row else None


def _quick_profile() -> None:
    """
    Lightweight, non-blocking profile for very large bronze tables.

    Avoids:
    - COUNT(*)
    - COUNT(DISTINCT ...)
    """
    est_total = _estimate_row_count("bronze")
    if est_total is None:
        logger.info("[SILVER] Bronze estimated rows: unavailable (stats not ready)")
    else:
        logger.info("[SILVER] Bronze estimated rows: %s", f"{est_total:,}")

    with get_connection() as conn:
        with conn.cursor() as cur:
            # NULL/empty key count can still be expensive, but is usually much cheaper
            # than full DISTINCT and gives actionable quality signal.
            cur.execute(
                """
                SELECT COUNT(*)
                FROM bronze
                WHERE transaction_id IS NULL OR transaction_id = '';
                """
            )
            null_or_empty = cur.fetchone()[0]
            logger.info(
                "[SILVER] Null/empty transaction_id rows: %s", f"{null_or_empty:,}"
            )

    logger.info(
        "[SILVER] Duplicate profiling skipped by default "
        "(enable with full_profile=True if needed)."
    )


def _full_profile() -> None:
    """
    Full profile with expensive metrics (blocking on very large datasets).
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM bronze;")
            total = cur.fetchone()[0]
            logger.info("[SILVER] Total rows in bronze: %s", f"{total:,}")

            cur.execute(
                """
                SELECT COUNT(*) - COUNT(DISTINCT transaction_id)
                FROM bronze;
                """
            )
            dups = cur.fetchone()[0]
            logger.info("[SILVER] Duplicate transaction_ids: %s", f"{dups:,}")

            cur.execute(
                """
                SELECT COUNT(*)
                FROM bronze
                WHERE transaction_id IS NULL OR transaction_id = '';
                """
            )
            nulls = cur.fetchone()[0]
            logger.info("[SILVER] Null/empty transaction_id: %s", f"{nulls:,}")


def clean_silver(full_profile: bool = False):
    """
    Build Silver from Bronze.

    Args:
        full_profile:
            - False (default): lightweight non-blocking profiling
            - True: run full (expensive) profiling including distinct duplicate count
    """
    logger.info("[SILVER] Starting silver cleaning (full_profile=%s)", full_profile)

    if full_profile:
        _full_profile()
    else:
        _quick_profile()

    # Helper index to speed deduplication/window operations
    with get_connection() as conn:
        with conn.cursor() as cur:
            logger.info("[SILVER] Creating helper index on bronze...")
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_bronze_txid_ts
                ON bronze (transaction_id, transaction_timestamp DESC);
                """
            )
            cur.execute("ANALYZE bronze;")
            logger.info("[SILVER] Bronze analyzed")

    # Create cleaned silver table
    with get_connection() as conn:
        with conn.cursor() as cur:
            logger.info("[SILVER] Rebuilding silver table...")
            cur.execute("DROP TABLE IF EXISTS silver CASCADE;")
            cur.execute(create_silver_table_sql())
            cur.execute("ANALYZE silver;")
            logger.info("[SILVER] Silver table created and analyzed")

    est_silver = _estimate_row_count("silver")
    if est_silver is not None:
        logger.info("[SILVER] Silver estimated rows: %s", f"{est_silver:,}")
    else:
        logger.info("[SILVER] Silver estimated rows: unavailable")


def main():
    """
    Default CLI behavior is non-blocking profile.
    """
    clean_silver(full_profile=False)


if __name__ == "__main__":
    main()
