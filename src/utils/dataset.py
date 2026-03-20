from pathlib import Path

import kagglehub

from utils.constants import DATASET_HANDLE


def get_dataset(output_dir: Path):
    kagglehub.dataset_download(
        DATASET_HANDLE, output_dir=str(output_dir), force_download=False
    )
