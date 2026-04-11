from dagster import asset, AssetExecutionContext, AssetIn

from src.config import settings
from src.silver import clean_silver


@asset(
    group_name="medallion",
    ins={"bronze_table": AssetIn()},
    description=(
        "Stream parquet files through bronze staging into silver. "
        "Uses DuckDB for processing when USE_DUCKDB=true, pandas otherwise."
    ),
)
def silver_table(context: AssetExecutionContext, bronze_table: None) -> None:
    engine = "DuckDB" if settings.use_duckdb else "pandas"
    context.log.info("Building silver layer (engine: %s)", engine)
    clean_silver(parquet_dir=str(settings.data_path))
    context.log.info("Silver layer complete")
