from contextlib import contextmanager

import duckdb
from dagster import ConfigurableResource

from src.config import settings


class DuckDBResource(ConfigurableResource):
    path: str = settings.duckdb_path

    @contextmanager
    def get_connection(self):
        conn = duckdb.connect(self.path)
        try:
            yield conn
        finally:
            conn.close()
