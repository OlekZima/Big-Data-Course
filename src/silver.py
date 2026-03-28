"""
Silver layer: storage-safe streaming build.

This module builds `silver` without materializing full `bronze` + `silver`
at the same time (which caused DiskFull).

Flow per parquet file:
1. Read parquet batch
2. Load raw rows into `bronze` staging (UNLOGGED)
3. Upsert cleaned rows from `bronze` -> `silver`
4. Truncate `bronze`
5. Repeat

Result:
- Bronze exists and is genuinely loaded (staging role)
- Silver is cleaned + deduplicated
- Peak disk stays close to: silver size + one parquet batch
"""

from __future__ import annotations

import io
from pathlib import Path

import pandas as pd
from psycopg2 import sql
from tqdm import tqdm

from .sql_queries import create_bronze_table_sql, create_silver_table_sql
from .utils.constants import (
    BRONZE_TABLE,
    SILVER_TABLE,
    TRANSACTION_COLUMNS,
    TRANSACTION_KEY_COLUMN,
    TRANSACTION_TIMESTAMP_COLUMN,
)
from .utils.db import get_connection
from .utils.logger import get_logger

logger = get_logger(__name__)

_COLS: list[str] = TRANSACTION_COLUMNS

_UPSERT_FROM_BRONZE_SQL = """
INSERT INTO silver (
    transaction_id,
    account_id,
    transaction_timestamp,
    mcc_code,
    channel,
    amount,
    txn_type,
    counterparty_id
)
SELECT
    b.transaction_id,
    b.account_id,
    b.transaction_timestamp,
    b.mcc_code,
    b.channel,
    b.amount,
    b.txn_type,
    b.counterparty_id
FROM {BRONZE_TABLE} b
WHERE b.{TRANSACTION_KEY_COLUMN} IS NOT NULL
  AND b.{TRANSACTION_KEY_COLUMN} <> ''
ON CONFLICT ({TRANSACTION_KEY_COLUMN}) DO UPDATE
SET
    account_id            = EXCLUDED.account_id,
    transaction_timestamp = EXCLUDED.transaction_timestamp,
    mcc_code              = EXCLUDED.mcc_code,
    channel               = EXCLUDED.channel,
    amount                = EXCLUDED.amount,
    txn_type              = EXCLUDED.txn_type,
    counterparty_id       = EXCLUDED.counterparty_id
WHERE {SILVER_TABLE}.{TRANSACTION_TIMESTAMP_COLUMN} IS NULL
   OR EXCLUDED.{TRANSACTION_TIMESTAMP_COLUMN} > {SILVER_TABLE}.{TRANSACTION_TIMESTAMP_COLUMN}
""".format(
    BRONZE_TABLE=BRONZE_TABLE,
    SILVER_TABLE=SILVER_TABLE,
    TRANSACTION_KEY_COLUMN=TRANSACTION_KEY_COLUMN,
    TRANSACTION_TIMESTAMP_COLUMN=TRANSACTION_TIMESTAMP_COLUMN,
)


def _estimate_row_count(table_name: str) -> int | None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT CASE
                         WHEN c.reltuples < 0 THEN NULL
                         ELSE c.reltuples::bigint
                       END
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = 'public'
                  AND c.relname = %s
                """,
                (table_name,),
            )
            row = cur.fetchone()
            return row[0] if row else None


def _prepare_df(df: pd.DataFrame) -> pd.DataFrame:
    # Ensure schema compatibility for drifted files
    for col in _COLS:
        if col not in df.columns:
            df[col] = None

    df = df.loc[:, _COLS].copy()

    # Keep only rows with a usable key before dedup/upsert
    key_mask: pd.Series = df[TRANSACTION_KEY_COLUMN].notna() & (
        df[TRANSACTION_KEY_COLUMN] != ""
    )
    df = df.loc[key_mask, :].copy()

    if df.empty:
        return df

    # Per-batch dedup: keep the latest timestamp per transaction_id.
    # Parse timestamps first so ordering is truly chronological.
    parsed_ts = pd.to_datetime(
        df[TRANSACTION_TIMESTAMP_COLUMN], errors="coerce", utc=True
    )
    df = (
        df.assign(_parsed_ts=parsed_ts)
        .sort_values(
            by=[TRANSACTION_KEY_COLUMN, "_parsed_ts"],
            ascending=[True, False],
            na_position="last",
        )
        .drop_duplicates(subset=[TRANSACTION_KEY_COLUMN], keep="first")
        .drop(columns=["_parsed_ts"])
    )

    # Normalize missing values for COPY NULL handling
    df = df.where(pd.notnull(df), None)
    return df


def _create_staging_bronze() -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {BRONZE_TABLE} CASCADE;")
            cur.execute(create_bronze_table_sql())
            cur.execute(f"TRUNCATE TABLE {BRONZE_TABLE};")


def _create_target_silver() -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {SILVER_TABLE} CASCADE;")
            cur.execute(create_silver_table_sql())


def _copy_df_to_bronze(df: pd.DataFrame) -> int:
    if df.empty:
        return 0

    buf = io.StringIO()
    df.to_csv(buf, sep="\t", header=False, na_rep="\\N", index=False)
    buf.seek(0)

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.copy_expert(
                sql.SQL(
                    f"COPY {BRONZE_TABLE} ({{}}) FROM STDIN "
                    "WITH (FORMAT CSV, DELIMITER E'\\t', NULL '\\N')"
                ).format(sql.SQL(", ").join(sql.Identifier(c) for c in _COLS)),
                buf,
            )
            return len(df)


def _upsert_silver_from_bronze() -> int:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(_UPSERT_FROM_BRONZE_SQL)
            return cur.rowcount


def _truncate_bronze() -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {BRONZE_TABLE};")


def clean_silver(parquet_dir: str = "data/raw", full_profile: bool = False) -> None:
    """
    Build silver by streaming parquet files through bronze staging.

    Args:
        parquet_dir: Directory with raw parquet files.
        full_profile: If True, runs exact COUNT(*) on silver at the end.
    """
    path = Path(parquet_dir)
    parquet_files = sorted(path.rglob("*.parquet"))

    if not parquet_files:
        logger.warning("[SILVER] No parquet files found in %s", parquet_dir)
        return

    logger.info(
        "[SILVER] Starting streaming silver build from %d files", len(parquet_files)
    )

    # Recreate target and staging schemas
    _create_target_silver()
    _create_staging_bronze()

    total_loaded_to_bronze = 0
    total_upserted_silver = 0

    for pf in tqdm(parquet_files, desc="Silver"):
        df = pd.read_parquet(pf)
        df = _prepare_df(df)

        loaded = _copy_df_to_bronze(df)
        total_loaded_to_bronze += loaded

        upserted = _upsert_silver_from_bronze()
        total_upserted_silver += upserted

        _truncate_bronze()

    # Planner stats
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"ANALYZE {SILVER_TABLE};")

    logger.info(
        "[SILVER] Stream complete | rows loaded to bronze: %s | rows upserted to silver: %s",
        f"{total_loaded_to_bronze:,}",
        f"{total_upserted_silver:,}",
    )

    if full_profile:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {SILVER_TABLE};")
                exact = cur.fetchone()[0]
                logger.info("[SILVER] Silver exact rows: %s", f"{exact:,}")
    else:
        est = _estimate_row_count(SILVER_TABLE)
        logger.info(
            "[SILVER] Silver estimated rows: %s",
            f"{est:,}" if est is not None else "unavailable",
        )

    # Keep bronze table (empty) to preserve raw layer presence in DB architecture
    logger.info("[SILVER] Bronze staging table retained (empty) after load")


def main() -> None:
    clean_silver()


if __name__ == "__main__":
    main()
