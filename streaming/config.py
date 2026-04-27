"""
Streaming configuration module.
"""

import os
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class StreamingSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Redis settings
    redis_host: str = Field(default="localhost", alias="REDIS_HOST")
    redis_port: int = Field(default=6379, alias="REDIS_PORT")
    redis_db: int = Field(default=0, alias="REDIS_DB")
    redis_password: str | None = Field(default=None, alias="REDIS_PASSWORD")

    # Queue settings
    queue_name: str = Field(default="bgd_ingestion", alias="QUEUE_NAME")
    batch_size: int = Field(default=1000, alias="BATCH_SIZE")
    max_queue_size: int = Field(default=10000, alias="MAX_QUEUE_SIZE")

    # File watcher settings
    watch_path: Path = Field(default=Path("data/raw"), alias="WATCH_PATH")
    watch_pattern: str = Field(default="*.parquet", alias="WATCH_PATTERN")
    poll_interval: float = Field(default=1.0, alias="POLL_INTERVAL")  # seconds

    # Ingestion settings
    ingestion_trigger: str = Field(default="file", alias="INGESTION_TRIGGER")  # file, schedule, manual
    schedule_interval: str = Field(default="*/5 * * * *", alias="SCHEDULE_INTERVAL")  # cron expression

    # Processing settings
    processing_workers: int = Field(default=os.cpu_count() or 1, alias="PROCESSING_WORKERS")
    processing_timeout: int = Field(default=300, alias="PROCESSING_TIMEOUT")  # seconds

    @property
    def redis_url(self) -> str:
        if self.redis_password:
            return f"redis://:{self.redis_password}@{self.redis_host}:{self.redis_port}/{self.redis_db}"
        else:
            return f"redis://{self.redis_host}:{self.redis_port}/{self.redis_db}"

    @property
    def redis_dict(self) -> dict:
        return {
            "host": self.redis_host,
            "port": self.redis_port,
            "db": self.redis_db,
            "password": self.redis_password,
        }


streaming_settings = StreamingSettings()
