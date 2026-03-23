"""
Bronze layer: raw ingestion schema definition.

In the full pipeline (src/main.py) bronze is used as a real staging table.
Each parquet batch is loaded into `bronze`, upserted into `silver`, and then
`bronze` is truncated before the next batch.
The table is created here (UNLOGGED, empty) to:
  • satisfy the ERD / deliverable requirement for a documented raw layer, and
  • give gold / reporting tools a stable schema reference if needed.

Bronze is retained as an empty staging table by src/silver.py after silver is
fully built.

Standalone usage:
    uv run python -m src.bronze
    # → creates the empty bronze schema in the database
"""

from .sql_queries import create_bronze_table_sql
from .utils.db import get_connection
from .utils.logger import get_logger

logger = get_logger(__name__)


def create_bronze_table(drop_if_exists: bool = True) -> None:
    """
    Create the bronze staging schema (UNLOGGED, empty).

    Args:
        drop_if_exists: When True (default) any existing bronze table is
                        dropped first so the schema is always fresh.
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            if drop_if_exists:
                cur.execute("DROP TABLE IF EXISTS bronze CASCADE;")
            cur.execute(create_bronze_table_sql())

    logger.info(
        "[BRONZE] UNLOGGED schema created (empty). "
        "Data streams through bronze transiently during silver build — "
        "see src/silver.py for the streaming ingestion logic."
    )


if __name__ == "__main__":
    create_bronze_table()
