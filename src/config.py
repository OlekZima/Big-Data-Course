import os
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    postgres_host: str = Field(default="localhost", alias="POSTGRES_HOST")
    postgres_port: int = Field(default=5432, alias="POSTGRES_PORT")
    postgres_db: str = Field(default="bigdata", alias="POSTGRES_DB")
    postgres_user: str = Field(default="user", alias="POSTGRES_USER")
    postgres_password: str = Field(default="pass", alias="POSTGRES_PASSWORD")

    duckdb_path: str = Field(default=":memory:", alias="DUCKDB_PATH")

    data_path: Path = Field(default=Path("data/raw"), alias="DATA_PATH")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    log_file: str = Field(default="logs/bgd.log", alias="LOG_FILE")

    use_duckdb: bool = Field(default=False, alias="USE_DUCKDB")
    use_dbt: bool = Field(default=False, alias="USE_DBT")
    # Parallel processing settings
    max_workers: int = Field(default=os.cpu_count() or 1, alias="MAX_WORKERS")
    duckdb_threads: int = Field(default=os.cpu_count() or 1, alias="DUCKDB_THREADS")

    @property
    def postgres_dsn(self) -> str:
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @property
    def postgres_dict(self) -> dict:
        return {
            "host": self.postgres_host,
            "port": self.postgres_port,
            "dbname": self.postgres_db,
            "user": self.postgres_user,
            "password": self.postgres_password,
        }


settings = Settings()
