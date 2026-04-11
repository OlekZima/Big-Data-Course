"""
DuckDB Processing Engine — single entry point demonstrating scalable parquet processing.

Reads raw parquet files from data/raw/, applies medallion-style transformations
(dedup, cleaning, aggregation) entirely in-memory using DuckDB, then reports stats.

Can be run standalone (no PostgreSQL required):
    python scripts/processing_engine.py

Or with a destination path for DuckDB persistence:
    DUCKDB_PATH=pipeline.duckdb python scripts/processing_engine.py
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

# ---------------------------------------------------------------------------
# Bootstrap: ensure project root on sys.path so `src.*` imports resolve
# ---------------------------------------------------------------------------
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import duckdb

from src.config import settings
from src.utils.constants import (
    TRANSACTION_COLUMNS,
    TRANSACTION_KEY_COLUMN,
    TRANSACTION_TIMESTAMP_COLUMN,
)
from src.utils.logger import get_logger

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Column helpers
# ---------------------------------------------------------------------------
_COLS = TRANSACTION_COLUMNS
_KEY = TRANSACTION_KEY_COLUMN
_TS = TRANSACTION_TIMESTAMP_COLUMN

_SELECT_COLS = ", ".join(f'"{c}"' for c in _COLS)


# ---------------------------------------------------------------------------
# Core processing steps
# ---------------------------------------------------------------------------

def _glob_pattern(data_path: Path) -> str:
    return str(data_path / "**" / "*.parquet")


def stage_bronze(conn: duckdb.DuckDBPyConnection, glob: str) -> str:
    """Load all parquet files into a raw bronze view."""
    conn.execute(f"""
        CREATE OR REPLACE VIEW bronze AS
        SELECT *
        FROM read_parquet('{glob}', union_by_name = true, hive_partitioning = false)
    """)
    row = conn.execute("SELECT COUNT(*) FROM bronze").fetchone()
    logger.info("[BRONZE] Raw rows: %s", f"{row[0]:,}")
    return "bronze"


def build_silver(conn: duckdb.DuckDBPyConnection) -> str:
    """
    Deduplicate bronze → silver.

    Strategy: latest transaction_timestamp wins per transaction_id.
    Rows without a key are dropped.
    """
    # Detect which columns actually exist in bronze
    available = {d[0] for d in conn.execute("DESCRIBE bronze").fetchall()}
    select_parts = [
        f'"{c}"' if c in available else f'NULL AS "{c}"'
        for c in _COLS
    ]
    select_expr = ", ".join(select_parts)

    conn.execute(f"""
        CREATE OR REPLACE TABLE silver AS
        WITH raw AS (
            SELECT {select_expr}
            FROM bronze
            WHERE "{_KEY}" IS NOT NULL
              AND CAST("{_KEY}" AS VARCHAR) <> ''
        ),
        deduped AS (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY "{_KEY}"
                    ORDER BY TRY_CAST("{_TS}" AS TIMESTAMPTZ) DESC NULLS LAST
                ) AS _rn
            FROM raw
        )
        SELECT {_SELECT_COLS}
        FROM deduped
        WHERE _rn = 1
    """)

    row = conn.execute("SELECT COUNT(*) FROM silver").fetchone()
    logger.info("[SILVER] Deduplicated rows: %s", f"{row[0]:,}")
    return "silver"


def build_gold(conn: duckdb.DuckDBPyConnection) -> dict[str, int]:
    """Build all six gold aggregation tables from silver."""
    gold_queries: dict[str, str] = {
        "gold_daily": f"""
            CREATE OR REPLACE TABLE gold_daily AS
            SELECT
                CAST("{_TS}" AS DATE)      AS txn_date,
                COUNT(*)                    AS txn_count,
                SUM(amount)                 AS total_amount,
                AVG(amount)                 AS avg_amount
            FROM silver
            WHERE "{_TS}" IS NOT NULL
            GROUP BY CAST("{_TS}" AS DATE)
            ORDER BY txn_date
        """,
        "gold_by_channel": f"""
            CREATE OR REPLACE TABLE gold_by_channel AS
            SELECT
                channel,
                COUNT(*)    AS txn_count,
                SUM(amount) AS total_amount,
                AVG(amount) AS avg_amount
            FROM silver
            GROUP BY channel
            ORDER BY txn_count DESC
        """,
        "gold_by_type": f"""
            CREATE OR REPLACE TABLE gold_by_type AS
            SELECT
                txn_type,
                COUNT(*)    AS txn_count,
                SUM(amount) AS total_amount,
                AVG(amount) AS avg_amount
            FROM silver
            GROUP BY txn_type
            ORDER BY txn_count DESC
        """,
        "gold_top_counterparties": f"""
            CREATE OR REPLACE TABLE gold_top_counterparties AS
            SELECT
                counterparty_id,
                COUNT(*)    AS txn_count,
                SUM(amount) AS total_amount
            FROM silver
            WHERE counterparty_id IS NOT NULL
            GROUP BY counterparty_id
            ORDER BY txn_count DESC
            LIMIT 100
        """,
        "gold_by_mcc": f"""
            CREATE OR REPLACE TABLE gold_by_mcc AS
            SELECT
                mcc_code,
                COUNT(*)    AS txn_count,
                SUM(amount) AS total_amount,
                AVG(amount) AS avg_amount
            FROM silver
            GROUP BY mcc_code
            ORDER BY txn_count DESC
        """,
        "gold_channel_share": f"""
            CREATE OR REPLACE TABLE gold_channel_share AS
            WITH totals AS (
                SELECT SUM(amount) AS grand_total FROM silver
            )
            SELECT
                channel,
                SUM(amount)                              AS channel_total,
                ROUND(100.0 * SUM(amount) / t.grand_total, 4) AS pct_of_total
            FROM silver
            CROSS JOIN totals t
            GROUP BY channel, t.grand_total
            ORDER BY channel_total DESC
        """,
    }

    counts: dict[str, int] = {}
    for name, sql in gold_queries.items():
        conn.execute(sql)
        row = conn.execute(f"SELECT COUNT(*) FROM {name}").fetchone()
        counts[name] = row[0]
        logger.info("[GOLD] %s: %s rows", name, f"{row[0]:,}")

    return counts


def print_sample(conn: duckdb.DuckDBPyConnection, table: str, limit: int = 5) -> None:
    logger.info("[SAMPLE] %s (top %d):", table, limit)
    conn.execute(f"SELECT * FROM {table} LIMIT {limit}").show()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run(data_path: Path, duckdb_path: str, show_samples: bool) -> None:
    glob = _glob_pattern(data_path)
    parquet_files = list(data_path.rglob("*.parquet"))

    if not parquet_files:
        logger.error("[ENGINE] No parquet files in %s — run pipeline first or set DATA_PATH.", data_path)
        sys.exit(1)

    logger.info("[ENGINE] DuckDB processing engine starting")
    logger.info("[ENGINE] Parquet files: %d | DB path: %s", len(parquet_files), duckdb_path)

    t0 = time.perf_counter()

    conn = duckdb.connect(duckdb_path)
    try:
        # Bronze
        stage_bronze(conn, glob)

        # Silver
        build_silver(conn)

        # Gold
        gold_counts = build_gold(conn)

        elapsed = time.perf_counter() - t0
        logger.info("[ENGINE] Done in %.2fs", elapsed)

        # Summary
        logger.info("=" * 60)
        logger.info("PIPELINE SUMMARY")
        logger.info("  Engine : DuckDB (in-process, columnar)")
        logger.info("  Source : %d parquet file(s) from %s", len(parquet_files), data_path)
        silver_count = conn.execute("SELECT COUNT(*) FROM silver").fetchone()[0]
        logger.info("  Silver : %s deduplicated transactions", f"{silver_count:,}")
        for table, count in gold_counts.items():
            logger.info("  %-30s %s rows", table + ":", f"{count:,}")
        logger.info("  Elapsed: %.2fs", elapsed)
        logger.info("=" * 60)

        if show_samples:
            print_sample(conn, "gold_daily")
            print_sample(conn, "gold_by_channel")
            print_sample(conn, "gold_channel_share")

    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="BGD DuckDB Processing Engine — medallion pipeline, no PostgreSQL required."
    )
    parser.add_argument(
        "--data-path",
        default=str(settings.data_path),
        help="Directory containing raw parquet files (default: %(default)s)",
    )
    parser.add_argument(
        "--duckdb-path",
        default=settings.duckdb_path,
        help="DuckDB DB file path, or ':memory:' (default: %(default)s)",
    )
    parser.add_argument(
        "--samples",
        action="store_true",
        default=False,
        help="Print sample rows from gold tables after run",
    )
    args = parser.parse_args()

    run(
        data_path=Path(args.data_path),
        duckdb_path=args.duckdb_path,
        show_samples=args.samples,
    )


if __name__ == "__main__":
    main()
