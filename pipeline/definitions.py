from dagster import Definitions, define_asset_job, AssetSelection

from .assets.bronze import bronze_table
from .assets.silver import silver_table
from .assets.gold import gold_tables
from .resources.postgres_resource import PostgresResource
from .resources.duckdb_resource import DuckDBResource
from src.config import settings

medallion_pipeline_job = define_asset_job(
    name="medallion_pipeline_job",
    selection=AssetSelection.all(),
    description="Full Bronze -> Silver -> Gold medallion pipeline.",
)

defs = Definitions(
    assets=[bronze_table, silver_table, gold_tables],
    jobs=[medallion_pipeline_job],
    resources={
        "postgres": PostgresResource(
            host=settings.postgres_host,
            port=settings.postgres_port,
            dbname=settings.postgres_db,
            user=settings.postgres_user,
            password=settings.postgres_password,
        ),
        "duckdb": DuckDBResource(
            path=settings.duckdb_path,
        ),
    },
)
