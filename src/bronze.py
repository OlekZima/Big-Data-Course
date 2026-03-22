"""
Bronze layer: load Parquet into PostgreSQL (FAST version).
Uses COPY command via pg.copy_expert for max speed.
"""

from pathlib import Path

import pandas as pd
from psycopg2 import sql
from tqdm import tqdm

from .sql_queries import create_bronze_table_sql
from .utils.db import get_connection
from .utils.logger import get_logger

logger = get_logger(__name__)


def create_bronze_table(drop_if_exists: bool = True):
    with get_connection() as conn:
        with conn.cursor() as cur:
            if drop_if_exists:
                cur.execute("DROP TABLE IF EXISTS bronze CASCADE;")
            cur.execute(create_bronze_table_sql())
    logger.info("[BRONZE] Table created")


def load_parquet_to_postgres(
    parquet_dir: str = "data/raw",
    drop_if_exists: bool = True,
    vacuum_after_load: bool = True,
):
    create_bronze_table(drop_if_exists=drop_if_exists)

    path = Path(parquet_dir)
    parquet_files = sorted(path.rglob("*.parquet"))
    logger.info("[BRONZE] Found %d Parquet files", len(parquet_files))

    cols = [
        "transaction_id",
        "account_id",
        "transaction_timestamp",
        "mcc_code",
        "channel",
        "amount",
        "txn_type",
        "counterparty_id",
    ]

    total = 0

    with get_connection() as conn:
        with conn.cursor() as cur:
            # Disable indexes for faster inserts
            cur.execute("SET session_replication_role = 'replica';")

            for pf in tqdm(parquet_files, desc="Loading"):
                # Read entire file as DataFrame
                df = pd.read_parquet(pf)

                # Handle missing columns gracefully
                for col in cols:
                    if col not in df.columns:
                        df[col] = None

                # Fill NaN with proper SQL NULL
                df = df[cols].where(pd.notnull(df[cols]), None)

                # Use copy_expert for maximum speed (like psql \copy)
                import io

                buffer = io.StringIO()
                df.to_csv(buffer, sep="\t", header=False, na_rep="\\N", index=False)
                buffer.seek(0)

                cur.copy_expert(
                    sql.SQL(
                        "COPY bronze ({}) FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', NULL '\\N')"
                    ).format(sql.SQL(", ").join(sql.Identifier(c) for c in cols)),
                    buffer,
                )
                conn.commit()
                total += len(df)

    # Re-enable indexes
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SET session_replication_role = 'origin';")

    # VACUUM must run outside transaction block
    if vacuum_after_load:
        with get_connection(autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute("VACUUM ANALYZE bronze;")

    logger.info("[BRONZE] Loaded %s rows", f"{total:,}")


if __name__ == "__main__":
    load_parquet_to_postgres()
