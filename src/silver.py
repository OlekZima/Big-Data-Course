"""
Silver layer: clean data and handle 3 key data quality issues.
"""

from utils.db import execute_sql, get_connection


def clean_silver():
    """
    Three data quality issues identified and resolved:

    1. DUPLICATES - transaction_id duplicated (same txn logged multiple times)
       Solution: DISTINCT on transaction_id

    2. NULL KEYS - rows with NULL transaction_id (invalid/broken records)
       Solution: WHERE transaction_id IS NOT NULL

    3. TIMESTAMP FORMAT - transaction_timestamp was ISO string, not TIMESTAMPTZ
       Solution: CAST / conversion during INSERT (bronze handles it)
    """

    # Report before cleaning
    with get_connection() as conn:
        with conn.cursor() as cur:
            # Count total
            cur.execute("SELECT COUNT(*) FROM bronze;")
            total = cur.fetchone()[0]
            print(f"[SILVER] Total rows in bronze: {total:,}")

            # Duplicates
            cur.execute("""
                SELECT COUNT(*) - COUNT(DISTINCT transaction_id)
                FROM bronze;
            """)
            dups = cur.fetchone()[0]
            print(f"[SILVER] Duplicate transaction_ids: {dups:,}")

            # Nulls
            cur.execute("""
                SELECT COUNT(*)
                FROM bronze
                WHERE transaction_id IS NULL OR transaction_id = '';
            """)
            nulls = cur.fetchone()[0]
            print(f"[SILVER] Null/empty transaction_id: {nulls:,}")

    # Create silver table (cleaned)
    sql = """
    CREATE TABLE IF NOT EXISTS silver AS
    SELECT DISTINCT ON (transaction_id)
           transaction_id,
           account_id,
           transaction_timestamp,
           mcc_code,
           channel,
           amount,
           txn_type,
           counterparty_id
    FROM bronze
    WHERE transaction_id IS NOT NULL
      AND transaction_id != '';
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS silver CASCADE;")
            cur.execute(sql)

    # Stats after
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM silver;")
            count = cur.fetchone()[0]
            print(f"[SILVER] Silver rows after cleaning: {count:,}")
            print(f"[SILVER] Removed: {total - count:,} rows (duplicates + nulls)")


if __name__ == "__main__":
    clean_silver()
