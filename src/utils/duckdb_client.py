from contextlib import contextmanager
from typing import Generator

import duckdb

from src.config import settings


@contextmanager
def get_duckdb_connection() -> Generator[duckdb.DuckDBPyConnection, None, None]:
    conn = duckdb.connect(settings.duckdb_path)
    try:
        yield conn
    finally:
        conn.close()


def read_parquet(path: str) -> duckdb.DuckDBPyRelation:
    conn = duckdb.connect(settings.duckdb_path)
    return conn.read_parquet(path)


def parquet_to_dataframe(path: str):
    with get_duckdb_connection() as conn:
        return conn.execute(f"SELECT * FROM read_parquet('{path}')").df()


def parquet_glob_to_dataframe(glob_pattern: str):
    with get_duckdb_connection() as conn:
        return conn.execute(
            f"SELECT * FROM read_parquet('{glob_pattern}', union_by_name=true)"
        ).df()
