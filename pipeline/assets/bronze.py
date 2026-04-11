from dagster import asset, AssetExecutionContext

from src.bronze import create_bronze_table


@asset(
    group_name="medallion",
    description="Create empty UNLOGGED bronze staging table in PostgreSQL.",
)
def bronze_table(context: AssetExecutionContext) -> None:
    context.log.info("Creating bronze staging table")
    create_bronze_table()
    context.log.info("Bronze table ready")
