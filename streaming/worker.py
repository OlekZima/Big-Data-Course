"""
Worker for processing queue items in the streaming pipeline.

This module contains a worker that processes items from the Redis queue,
performing the silver layer processing on each file.
"""

import json
import logging
import signal
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from psycopg2 import sql

from src.config import settings
from src.sql_queries import create_bronze_table_sql, create_silver_table_sql
from src.utils.constants import (
    BRONZE_TABLE,
    SILVER_TABLE,
    TRANSACTION_COLUMNS,
    TRANSACTION_KEY_COLUMN,
    TRANSACTION_TIMESTAMP_COLUMN,
)
from src.utils.db import get_connection
from src.utils.duckdb_client import get_duckdb_connection
from src.utils.logger import get_logger

from .config import streaming_settings
from .queue_manager import queue_manager

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


class StreamingWorker:
    """Worker that processes queue items and performs silver layer processing."""

    def __init__(self, worker_id: Optional[str] = None):
        """
        Initialize the worker.

        Args:
            worker_id: Unique identifier for this worker. If None, a default is generated.
        """
        self.worker_id = worker_id or f"worker-{int(time.time())}"
        self.running = False
        self.processed_count = 0
        self.error_count = 0

        # Ensure bronze and silver tables exist
        self._ensure_tables_exist()

    def _ensure_tables_exist(self) -> None:
        """Ensure bronze and silver tables exist in the database."""
        try:
            with get_connection() as conn:
                with conn.cursor() as cur:
                    # Check if bronze table exists
                    cur.execute("""
                        SELECT EXISTS (
                            SELECT FROM information_schema.tables
                            WHERE table_schema = 'public'
                            AND table_name = %s
                        )
                    """, (BRONZE_TABLE,))
                    bronze_exists = cur.fetchone()[0]

                    if not bronze_exists:
                        logger.info(f"Creating bronze table: {BRONZE_TABLE}")
                        cur.execute(create_bronze_table_sql())

                    # Check if silver table exists
                    cur.execute("""
                        SELECT EXISTS (
                            SELECT FROM information_schema.tables
                            WHERE table_schema = 'public'
                            AND table_name = %s
                        )
                    """, (SILVER_TABLE,))
                    silver_exists = cur.fetchone()[0]

                    if not silver_exists:
                        logger.info(f"Creating silver table: {SILVER_TABLE}")
                        cur.execute(create_silver_table_sql())

        except Exception as e:
            logger.error(f"Failed to ensure tables exist: {e}")
            raise

    def _prepare_df_duckdb(self, parquet_path: Path) -> pd.DataFrame:
        """Read and clean a parquet file using DuckDB as processing engine."""
        with get_duckdb_connection() as conn:
            raw = conn.execute(
                f"SELECT * FROM read_parquet('{parquet_path}')"
            ).description
            available = {d[0] for d in raw}

            select_parts = [
                f'"{c}"' if c in available else f"NULL AS \"{c}\""
                for c in _COLS
            ]
            select_expr = ", ".join(select_parts)

            df: pd.DataFrame = conn.execute(
                f"""
                WITH raw AS (
                    SELECT {select_expr}
                    FROM read_parquet('{parquet_path}')
                    WHERE "{TRANSACTION_KEY_COLUMN}" IS NOT NULL
                      AND CAST("{TRANSACTION_KEY_COLUMN}" AS VARCHAR) <> ''
                ),
                deduped AS (
                    SELECT *,
                        ROW_NUMBER() OVER (
                            PARTITION BY "{TRANSACTION_KEY_COLUMN}"
                            ORDER BY TRY_CAST("{TRANSACTION_TIMESTAMP_COLUMN}" AS TIMESTAMPTZ) DESC NULLS LAST
                        ) AS _rn
                    FROM raw
                )
                SELECT {", ".join(f'"{c}"' for c in _COLS)}
                FROM deduped
                WHERE _rn = 1
                """
            ).df()

        df = df.where(pd.notnull(df), None)
        return df

    def _prepare_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """Prepare a DataFrame for loading (pandas path)."""
        for col in _COLS:
            if col not in df.columns:
                df[col] = None

        df = df.loc[:, _COLS].copy()

        key_mask: pd.Series = df[TRANSACTION_KEY_COLUMN].notna() & (
            df[TRANSACTION_KEY_COLUMN] != ""
        )
        df = df.loc[key_mask, :].copy()

        if df.empty:
            return df

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

        df = df.where(pd.notnull(df), None)
        return df

    def _copy_df_to_bronze(self, df: pd.DataFrame) -> int:
        """Copy a DataFrame to the bronze staging table."""
        if df.empty:
            return 0

        import io
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

    def _upsert_silver_from_bronze(self) -> int:
        """Upsert data from bronze to silver."""
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(_UPSERT_FROM_BRONZE_SQL)
                return cur.rowcount

    def _truncate_bronze(self) -> None:
        """Truncate the bronze staging table."""
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"TRUNCATE TABLE {BRONZE_TABLE};")

    def process_file(self, file_path: Path) -> Dict[str, Any]:
        """
        Process a single Parquet file.

        Args:
            file_path: Path to the Parquet file.

        Returns:
            Dictionary with processing results.
        """
        start_time = time.time()
        logger.info(f"[{self.worker_id}] Processing file: {file_path}")

        try:
            # Read and clean the file
            if settings.use_duckdb:
                df = self._prepare_df_duckdb(file_path)
            else:
                df = pd.read_parquet(file_path)
                df = self._prepare_df(df)

            # Load to bronze and upsert to silver
            loaded = self._copy_df_to_bronze(df)
            upserted = self._upsert_silver_from_bronze()
            self._truncate_bronze()

            processing_time = time.time() - start_time

            result = {
                "status": "success",
                "file_path": str(file_path),
                "rows_loaded": loaded,
                "rows_upserted": upserted,
                "processing_time": processing_time,
                "worker_id": self.worker_id,
                "timestamp": time.time(),
            }

            logger.info(
                f"[{self.worker_id}] Processed {file_path}: "
                f"loaded={loaded}, upserted={upserted}, time={processing_time:.2f}s"
            )

            self.processed_count += 1
            return result

        except Exception as e:
            processing_time = time.time() - start_time
            logger.error(f"[{self.worker_id}] Failed to process {file_path}: {e}")
            self.error_count += 1

            return {
                "status": "error",
                "file_path": str(file_path),
                "error": str(e),
                "processing_time": processing_time,
                "worker_id": self.worker_id,
                "timestamp": time.time(),
            }

    def process_message(self, message: bytes) -> Dict[str, Any]:
        """
        Process a message from the queue.

        Args:
            message: The message bytes from the queue.

        Returns:
            Dictionary with processing results.
        """
        try:
            # Parse the message
            message_data = json.loads(message.decode('utf-8'))
            message_type = message_data.get("type")

            if message_type == "file":
                file_path = Path(message_data["path"])
                if file_path.exists():
                    return self.process_file(file_path)
                else:
                    return {
                        "status": "error",
                        "error": f"File does not exist: {file_path}",
                        "worker_id": self.worker_id,
                        "timestamp": time.time(),
                    }
            else:
                return {
                    "status": "error",
                    "error": f"Unknown message type: {message_type}",
                    "worker_id": self.worker_id,
                    "timestamp": time.time(),
                }

        except json.JSONDecodeError as e:
            return {
                "status": "error",
                "error": f"Failed to parse message as JSON: {e}",
                "worker_id": self.worker_id,
                "timestamp": time.time(),
            }
        except Exception as e:
            return {
                "status": "error",
                "error": f"Unexpected error processing message: {e}",
                "worker_id": self.worker_id,
                "timestamp": time.time(),
            }

    def run(self, timeout: Optional[int] = None) -> None:
        """
        Run the worker, processing messages from the queue.

        Args:
            timeout: Timeout in seconds for the worker to run. If None, runs indefinitely.
        """
        self.running = True
        start_time = time.time()

        logger.info(f"[{self.worker_id}] Starting worker")

        # Set up signal handlers for graceful shutdown
        def signal_handler(signum, frame):
            logger.info(f"[{self.worker_id}] Received signal {signum}, shutting down...")
            self.running = False

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        try:
            while self.running:
                # Check timeout
                if timeout is not None and (time.time() - start_time) > timeout:
                    logger.info(f"[{self.worker_id}] Timeout reached, stopping...")
                    break

                # Get a message from the queue
                message = queue_manager.pop(timeout=1)  # Block for up to 1 second

                if message is not None:
                    # Process the message
                    result = self.process_message(message)

                    # Log the result
                    if result["status"] == "success":
                        logger.debug(
                            f"[{self.worker_id}] Successfully processed {result['file_path']}"
                        )
                    else:
                        logger.error(
                            f"[{self.worker_id}] Failed to process message: {result.get('error')}"
                        )

                # Small sleep to prevent busy waiting
                time.sleep(0.1)

        except Exception as e:
            logger.error(f"[{self.worker_id}] Worker encountered an error: {e}")
            self.running = False
            raise

        finally:
            self.running = False
            logger.info(
                f"[{self.worker_id}] Worker stopped. "
                f"Processed: {self.processed_count}, Errors: {self.error_count}"
            )

    def stop(self) -> None:
        """Stop the worker."""
        self.running = False


def start_worker(worker_id: Optional[str] = None, timeout: Optional[int] = None) -> None:
    """
    Start a worker.

    Args:
        worker_id: Unique identifier for the worker.
        timeout: Timeout in seconds for the worker to run.
    """
    worker = StreamingWorker(worker_id=worker_id)
    worker.run(timeout=timeout)


if __name__ == "__main__":
    # Parse command line arguments
    import argparse

    parser = argparse.ArgumentParser(description="Start a streaming worker.")
    parser.add_argument(
        "--worker-id",
        type=str,
        help="Unique identifier for this worker.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        help="Timeout in seconds for the worker to run.",
    )

    args = parser.parse_args()

    # Start the worker
    start_worker(worker_id=args.worker_id, timeout=args.timeout)
