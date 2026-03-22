from contextlib import contextmanager

import psycopg2

DATABASE_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "bigdata",
    "user": "user",
    "password": "pass",
}


@contextmanager
def get_connection(autocommit: bool = False):
    conn = psycopg2.connect(**DATABASE_CONFIG)
    conn.autocommit = autocommit
    try:
        yield conn
        if not autocommit:
            conn.commit()
    except Exception:
        if not autocommit:
            conn.rollback()
        raise
    finally:
        conn.close()


def execute_sql(sql: str, params=None, autocommit: bool = False):
    with get_connection(autocommit=autocommit) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or {})
            if cur.description:
                return cur.fetchall()
