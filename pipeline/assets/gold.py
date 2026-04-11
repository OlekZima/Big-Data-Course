import subprocess
from pathlib import Path

from dagster import asset, AssetExecutionContext, AssetIn

from src.config import settings
from src.gold import create_gold_tables
from src.utils.constants import GOLD_TABLES

DBT_DIR = Path(__file__).parent.parent.parent / "dbt"


@asset(
    group_name="medallion",
    ins={"silver_table": AssetIn()},
    description=(
        "Build all six gold aggregation tables from silver. "
        "Uses dbt when USE_DBT=true, Python otherwise."
    ),
)
def gold_tables(context: AssetExecutionContext, silver_table: None) -> None:
    if settings.use_dbt:
        context.log.info("Building gold layer via dbt (dbt_dir=%s)", DBT_DIR)
        result = subprocess.run(
            [
                "uv", "run", "dbt", "run",
                "--profiles-dir", str(DBT_DIR),
                "--project-dir", str(DBT_DIR),
            ],
            capture_output=True,
            text=True,
        )
        context.log.info(result.stdout)
        if result.returncode != 0:
            context.log.error(result.stderr)
            raise RuntimeError(f"dbt run failed:\n{result.stderr}")
        context.log.info("dbt gold layer complete")
    else:
        context.log.info("Building gold layer via Python (%d tables)", len(GOLD_TABLES))
        create_gold_tables()
        context.log.info("Gold layer complete: %s", ", ".join(GOLD_TABLES))
