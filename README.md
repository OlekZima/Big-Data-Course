# BGD — Medallion ELT Pipeline (PostgreSQL)

This project implements a medallion architecture (Bronze → Silver → Gold) ELT pipeline on a transactional dataset. It includes reproducible SQL, Python loaders, and documentation with architecture diagrams.

## Deliverables

- **Problem Statement**: `docs/report.md`
- **DB Architecture (ERD + High Level)**: embedded below in Mermaid
- **Reproducible SQL script**: `sql/elt_pipeline.sql`
- **Code (versioned)**: `src/`
- **Data Quality Risks**: documented in `docs/report.md` and `data/README.md`

## Project Structure

```
BGD/
├── sql/
│   ├── elt_queries.sql
│   └── elt_pipeline.sql
├── src/
│   ├── bronze.py
│   ├── silver.py
│   ├── gold.py
│   ├── job.py
│   ├── main.py
│   └── sql_queries.py
├── docs/
│   ├── report.md
│   └── diagrams/
│       ├── erd.drawio
│       └── architecture.drawio
└── docker-compose.yml
```

## Architecture Diagrams (Mermaid)

### ERD (Medallion Tables)

```mermaid
erDiagram
    BRONZE {
        text transaction_id
        text account_id
        timestamptz transaction_timestamp
        bigint mcc_code
        text channel
        double amount
        text txn_type
        text counterparty_id
        timestamptz loaded_at
    }

    SILVER {
        text transaction_id
        text account_id
        timestamptz transaction_timestamp
        bigint mcc_code
        text channel
        double amount
        text txn_type
        text counterparty_id
    }

    GOLD_DAILY {
        date day
        bigint txn_count
        bigint unique_accounts
        double total_amount
        double avg_amount
        double min_amount
        double max_amount
    }

    GOLD_BY_CHANNEL {
        text channel
        bigint txn_count
        double total_amount
        double avg_amount
    }

    GOLD_BY_TYPE {
        text txn_type
        bigint txn_count
        double total_amount
        double avg_amount
    }

    GOLD_TOP_COUNTERPARTIES {
        text counterparty_id
        bigint txn_count
        double total_volume
    }

    GOLD_BY_MCC {
        bigint mcc_code
        bigint txn_count
        double total_amount
    }

    GOLD_CHANNEL_SHARE {
        text channel
        bigint txn_count
        double total_amount
        numeric txn_share
        double amount_share
    }

    BRONZE ||--|| SILVER : cleans_to
    SILVER ||--o{ GOLD_DAILY : aggregates_to
    SILVER ||--o{ GOLD_BY_CHANNEL : aggregates_to
    SILVER ||--o{ GOLD_BY_TYPE : aggregates_to
    SILVER ||--o{ GOLD_TOP_COUNTERPARTIES : aggregates_to
    SILVER ||--o{ GOLD_BY_MCC : aggregates_to
    SILVER ||--o{ GOLD_CHANNEL_SHARE : joins_and_aggregates_to
```

### High-Level Architecture

```mermaid
flowchart LR
    A[Kaggle Dataset / Parquet Files] --> B[Bronze Layer: raw table in PostgreSQL]
    B --> C[Silver Layer: cleaned & deduplicated table]
    C --> D[Gold Layer: curated marts and KPIs]

    D --> D1[gold_daily]
    D --> D2[gold_by_channel]
    D --> D3[gold_by_type]
    D --> D4[gold_top_counterparties]
    D --> D5[gold_by_mcc]
    D --> D6[gold_channel_share JOIN]

    subgraph Orchestration
        E[src/main.py]
        F[src/bronze.py]
        G[src/silver.py]
        H[src/gold.py]
    end

    E --> F --> G --> H
```

## Requirements

- Docker + Docker Compose
- Python 3.10+
- `uv` (recommended) or standard venv + pip

## Setup

1. Start PostgreSQL:

```
docker compose up -d
```

2. Install dependencies:

```
uv sync
```

## Run the pipeline

```
uv run python -m src.main
```

This will:

- download/prepare dataset (if missing),
- load Bronze,
- clean Silver,
- build Gold tables.

## Non-destructive run options

If you already have data in PostgreSQL and want to avoid dropping/reloading `bronze`, run only downstream steps:

```
uv run python -m src.silver
uv run python -m src.gold
```

You can also run specific layers independently:

```
uv run python -m src.bronze
uv run python -m src.silver
uv run python -m src.gold
```

## Runnable scripts (ordered)

All scripts below include a `if __name__ == "__main__":` entry point and can be executed directly:

1. `src/bronze.py` — create and load the Bronze table
2. `src/silver.py` — clean and create the Silver table
3. `src/gold.py` — build Gold tables (aggregations + JOIN-based)
4. `src/sql_queries.py` — list available SQL query names
5. `src/job.py` — Spark job (writes Silver/Gold parquet)
6. `src/main.py` — end-to-end orchestration

## SQL-Only Reproducible Pipeline

If you want to re-run the pipeline purely via SQL (tables only):

```
psql -h localhost -U user -d bigdata -f sql/elt_pipeline.sql
```

## Data Quality Risks (summary)

1. Duplicate transactions (same `transaction_id`)
2. Null/empty keys
3. Timestamp formatting inconsistencies

Full details: `docs/report.md` and `data/README.md`.

## Operational note: VACUUM behavior

`VACUUM ANALYZE` is executed outside transaction blocks (autocommit mode) to avoid PostgreSQL errors like:

- `VACUUM cannot run inside a transaction block`

This is handled in the Bronze loading implementation.
