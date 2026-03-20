import re
import shutil
from pathlib import Path


def flatten_files(input_dir: Path, output_dir: Path | None = None):
    """
    Flattens a directory structure by moving all files from nested directories
    to a single output directory.

    Args:
        input_dir: The root directory to flatten.
        output_dir: The directory where all files will be moved.
    """
    if output_dir is None:
        output_dir = input_dir

    if not output_dir.exists():
        output_dir.mkdir(parents=True)

    for item in input_dir.rglob("*.parquet"):
        if item.is_file():
            shutil.move(item, output_dir / item.name)


def remove_non_dataset_files(input_dir: Path):
    """
    Removes all files from a directory that are not dataset related .parquet files.

    Args:
        input_dir: The directory to clean up.
    """
    for item in input_dir.rglob("*"):
        if not re.match("part_\d{4}a?b?.parquet", item.name):
            match item.is_file():
                case True:
                    item.unlink()
                case False:
                    if not any(item.iterdir()):
                        item.rmdir()
                    else:
                        for sub_item in item.iterdir():
                            if sub_item.is_file():
                                sub_item.unlink()
            print(f"Removed: {item}")
