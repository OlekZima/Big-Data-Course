import psycopg2
from contextlib import contextmanager
from dagster import ConfigurableResource

from src.config import settings


class PostgresResource(ConfigurableResource):
    host: str = settings.postgres_host
    port: int = settings.postgres_port
    dbname: str = settings.postgres_db
    user: str = settings.postgres_user
    password: str = settings.postgres_password

    @contextmanager
    def get_connection(self):
        conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            dbname=self.dbname,
            user=self.user,
            password=self.password,
        )
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
