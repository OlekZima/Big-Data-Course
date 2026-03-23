"""
SQL query catalog and loader for the ELT pipeline.

All queries are stored in `sql/elt_queries.sql` and exposed through
documented functions for discoverability and pydoc generation.
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict

from .utils.logger import get_logger

_SQL_FILE = Path(__file__).resolve().parents[1] / "sql" / "elt_queries.sql"
_QUERY_CACHE: Dict[str, str] | None = None

logger = get_logger(__name__)


def _load_query_map() -> Dict[str, str]:
    """
    Parse `sql/elt_queries.sql` into a dictionary keyed by query name.

    The file must contain blocks prefixed by:
    `-- name: <query_name>`
    """
    sql_text = _SQL_FILE.read_text(encoding="utf-8")
    queries: Dict[str, str] = {}
    current_name = None
    current_lines: list[str] = []

    for line in sql_text.splitlines():
        if line.strip().lower().startswith("-- name:"):
            if current_name:
                queries[current_name] = "\n".join(current_lines).strip()
                current_lines = []
            current_name = line.split(":", 1)[1].strip()
        else:
            if current_name:
                current_lines.append(line)

    if current_name:
        queries[current_name] = "\n".join(current_lines).strip()

    return queries


def get_query(name: str) -> str:
    """
    Return a named SQL query from `sql/elt_queries.sql`.
    """
    global _QUERY_CACHE
    if _QUERY_CACHE is None:
        _QUERY_CACHE = _load_query_map()

    if name not in _QUERY_CACHE:
        raise KeyError(f"Query not found: {name}")

    return _QUERY_CACHE[name]


def create_bronze_table_sql() -> str:
    """
    Create the raw `bronze` table used for ingestion.

    The table captures raw transaction fields plus a load timestamp.
    """
    return get_query("create_bronze_table")


def create_silver_table_sql() -> str:
    """
    Create the `silver` table schema (UNLOGGED, with PK on `transaction_id`).

    Data cleaning and deduplication are applied during streaming upsert from
    bronze staging in the Silver pipeline implementation.
    """
    return get_query("create_silver_table")


def create_gold_daily_sql() -> str:
    """
    Create `gold_daily` aggregations by day.

    Includes transaction count, distinct accounts, and summary stats.
    """
    return get_query("create_gold_daily")


def create_gold_by_channel_sql() -> str:
    """
    Create `gold_by_channel` aggregations by channel.

    Provides transaction counts and amount metrics for each channel.
    """
    return get_query("create_gold_by_channel")


def create_gold_by_type_sql() -> str:
    """
    Create `gold_by_type` aggregations by transaction type.

    Summarizes counts and amount metrics for each txn_type.
    """
    return get_query("create_gold_by_type")


def create_gold_top_counterparties_sql() -> str:
    """
    Create `gold_top_counterparties` (top 20 by total volume).

    Filters out null/empty counterparties.
    """
    return get_query("create_gold_top_counterparties")


def create_gold_by_mcc_sql() -> str:
    """
    Create `gold_by_mcc` (top 20 MCC codes by total amount).
    """
    return get_query("create_gold_by_mcc")


def create_gold_channel_share_sql() -> str:
    """
    Create `gold_channel_share` with JOIN-based channel share metrics.

    Computes per-channel share of transactions and amount totals.
    """
    return get_query("create_gold_channel_share")


def main() -> None:
    """
    Print available SQL query names from the catalog.
    """
    query_map = _load_query_map()
    for name in sorted(query_map):
        logger.info("Available SQL query: %s", name)


if __name__ == "__main__":
    main()
