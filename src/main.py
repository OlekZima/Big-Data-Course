from pathlib import Path

from .bronze import create_bronze_table
from .gold import create_gold_tables
from .silver import clean_silver
from .utils.constants import DATA_PATH
from .utils.dataset import get_dataset
from .utils.files import flatten_files, remove_non_dataset_files
from .utils.logger import get_logger

logger = get_logger(__name__)


def main():
    if not DATA_PATH.exists() or not list(DATA_PATH.glob("*.parquet")):
        logger.info(
            "[MAIN] Dataset not found in %s. Downloading and preparing files...",
            DATA_PATH,
        )
        get_dataset(Path("data/raw"))
        flatten_files(Path("data/raw"))
        remove_non_dataset_files(Path("data/raw"))
    else:
        logger.info("[MAIN] Data already exists in %s, skipping download", DATA_PATH)

    logger.info("=== BRONZE ===")
    create_bronze_table()

    logger.info("=== SILVER ===")
    clean_silver(str(DATA_PATH))

    logger.info("=== GOLD ===")
    create_gold_tables()

    logger.info("[MAIN] Done! Tables: bronze (staging, retained empty), silver, gold_*")


if __name__ == "__main__":
    main()
