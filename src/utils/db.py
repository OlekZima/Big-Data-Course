import os
from contextlib import contextmanager
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values

DATABASE_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "bigdata",
    "user": "user",
    "password": "pass",
}


@contextmanager
def get_connection():
    conn = psycopg2.connect(**DATABASE_CONFIG)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def execute_sql(sql: str, params=None):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or {})
            if cur.description:
                return cur.fetchall()
