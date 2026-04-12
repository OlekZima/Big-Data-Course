from pathlib import Path

DATASET_HANDLE = "abhyudayrbih/rbih-nfpc-phase-2"
DATA_PATH = Path("data/raw")

BRONZE_TABLE = "bronze"
SILVER_TABLE = "silver"

GOLD_DAILY_TABLE = "gold_daily"
GOLD_BY_CHANNEL_TABLE = "gold_by_channel"
GOLD_BY_TYPE_TABLE = "gold_by_type"
GOLD_TOP_COUNTERPARTIES_TABLE = "gold_top_counterparties"
GOLD_BY_MCC_TABLE = "gold_by_mcc"
GOLD_CHANNEL_SHARE_TABLE = "gold_channel_share"

GOLD_TABLES = [
    GOLD_DAILY_TABLE,
    GOLD_BY_CHANNEL_TABLE,
    GOLD_BY_TYPE_TABLE,
    GOLD_TOP_COUNTERPARTIES_TABLE,
    GOLD_BY_MCC_TABLE,
    GOLD_CHANNEL_SHARE_TABLE,
]

TRANSACTION_COLUMNS = [
    "transaction_id",
    "account_id",
    "transaction_timestamp",
    "mcc_code",
    "channel",
    "amount",
    "txn_type",
    "counterparty_id",
]

TRANSACTION_KEY_COLUMN = "transaction_id"
TRANSACTION_TIMESTAMP_COLUMN = "transaction_timestamp"
