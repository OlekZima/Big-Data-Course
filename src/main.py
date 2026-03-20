from pathlib import Path

from bronze import load_parquet_to_postgres
from gold import create_gold_tables
from silver import clean_silver
from utils.constants import DATA_PATH
from utils.dataset import get_dataset
from utils.files import flatten_files, remove_non_dataset_files


def main():
    if not DATA_PATH.exists() or not list(DATA_PATH.glob("*.parquet")):
        get_dataset(Path("data/raw"))
        flatten_files(Path("data/raw"))
        remove_non_dataset_files(Path("data/raw"))
    else:
        print(f"[MAIN] Data already exists in {DATA_PATH}, skipping download")

    print("\n=== BRONZE ===")
    load_parquet_to_postgres("data/raw")

    print("\n=== SILVER ===")
    clean_silver()

    print("\n=== GOLD ===")
    create_gold_tables()

    print("\n[MAIN] Done! Tables: bronze, silver, gold_*")


if __name__ == "__main__":
    main()
