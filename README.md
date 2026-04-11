# BGD — Medallion ELT Pipeline

This project implements a medallion architecture (Bronze → Silver → Gold) ELT pipeline on a transactional dataset. It uses **PostgreSQL** as the data store, **DuckDB** as a scalable in-process processing engine, **Dagster** for orchestration, and **dbt** for gold-layer transformations.

## Deliverables

- **Problem Statement**: `docs/report.md`
- **DB Architecture (ERD + High Level)**: embedded below in Mermaid
- **Reproducible SQL script**: `sql/elt_pipeline.sql`
- **Code (versioned)**: `src/`, `pipeline/`, `dbt/`, `scripts/`
- **Data Quality Risks**: documented in `docs/report.md`

## Project Structure

```
BGD/
├── src/                        # Core ELT modules
│   ├── main.py                 # End-to-end orchestration (Python)
│   ├── bronze.py               # Bronze layer
│   ├── silver.py               # Silver layer (pandas or DuckDB path)
│   ├── gold.py                 # Gold layer (SQL)
│   ├── config.py               # Pydantic Settings (reads .env)
│   ├── sql_queries.py          # Named SQL loader
│   └── utils/
│       ├── db.py               # PostgreSQL connection manager
│       ├── duckdb_client.py    # DuckDB connection manager
│       └── constants.py        # Table names, column lists
├── pipeline/                   # Dagster orchestration
│   ├── definitions.py          # Dagster Definitions (assets, job, resources)
│   ├── assets/                 # bronze_table, silver_table, gold_tables
│   └── resources/              # PostgresResource, DuckDBResource
├── dbt/                        # dbt gold-layer transformations
│   ├── dbt_project.yml
│   ├── profiles.yml            # connection from env vars
│   └── models/marts/           # 6 gold models + schema tests
├── scripts/
│   └── processing_engine.py   # Standalone DuckDB engine (no PostgreSQL needed)
├── sql/
│   ├── elt_queries.sql         # All DDL + named queries
│   └── elt_pipeline.sql
├── docs/
│   └── report.md
├── .env.example                # Environment variable template
└── docker-compose.yml          # PostgreSQL 15
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
flowchart TD
    DS[("Kaggle Dataset\nParquet files\ndata/raw/")]

    subgraph ENGINE["DuckDB Processing Engine (scripts/processing_engine.py)"]
        direction LR
        DE_B[Bronze view\nread_parquet glob]
        DE_S[Silver\ndedup ROW_NUMBER]
        DE_G[Gold tables\n6 aggregations]
        DE_B --> DE_S --> DE_G
    end

    subgraph DAGSTER["Dagster Orchestrator (pipeline/)"]
        direction LR
        DA_B[bronze_table\nasset]
        DA_S[silver_table\nasset]
        DA_G[gold_tables\nasset]
        DA_B --> DA_S --> DA_G
    end

    subgraph PG["PostgreSQL (Docker)"]
        direction TB
        PG_B[(bronze\nUNLOGGED staging)]
        PG_S[(silver\nUNLOGGED deduped)]
        PG_G1[(gold_daily)]
        PG_G2[(gold_by_channel)]
        PG_G3[(gold_by_type)]
        PG_G4[(gold_top_counterparties)]
        PG_G5[(gold_by_mcc)]
        PG_G6[(gold_channel_share)]
    end

    subgraph DBT["dbt (dbt/)"]
        DBT_M[6 gold models\nschema tests]
    end

    DS -->|pandas or DuckDB| DA_S
    DS --> ENGINE

    DA_B -->|CREATE UNLOGGED TABLE| PG_B
    DA_S -->|COPY + ON CONFLICT upsert| PG_S
    DA_G -->|USE_DBT=false: CREATE TABLE AS| PG_G1 & PG_G2 & PG_G3 & PG_G4 & PG_G5 & PG_G6
    DA_G -->|USE_DBT=true: dbt run| DBT_M
    DBT_M -->|CREATE TABLE| PG_G1 & PG_G2 & PG_G3 & PG_G4 & PG_G5 & PG_G6
```

### Data Flow (Silver Streaming)

```mermaid
sequenceDiagram
    participant P as Parquet file
    participant D as DuckDB (optional)
    participant B as Bronze (PG staging)
    participant S as Silver (PG)

    loop per file (398 files)
        P->>D: read_parquet + dedup (USE_DUCKDB=true)
        P-->>B: pandas read + COPY (USE_DUCKDB=false)
        D-->>B: COPY cleaned batch
        B->>S: ON CONFLICT DO UPDATE (latest timestamp wins)
        B->>B: TRUNCATE
    end
    Note over B,S: Peak disk ≈ silver + one batch
```

## Requirements

- Docker + Docker Compose
- Python 3.10+
- `uv` package manager

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
- load Bronze (UNLOGGED — no WAL overhead),
- clean Silver (UNLOGGED) by streaming each parquet file through Bronze staging (load -> upsert -> truncate),
- build Gold tables.

> **Storage note:** Bronze is a transient staging table used per batch during
> Silver build. Each file is loaded into Bronze, upserted into Silver, then
> Bronze is truncated. This keeps peak usage close to `silver + one batch`.

## Non-destructive run options

`src/silver` recreates both `silver` and `bronze` as part of its own run.
If you only want to rebuild Gold from an existing Silver table, run:

```
uv run python -m src.gold
```

To rebuild Silver from raw parquet files:

```
uv run python -m src.silver
```

You can also run specific layers independently:

```
uv run python -m src.bronze
uv run python -m src.silver
uv run python -m src.gold
```

## Runnable scripts (ordered)

All scripts below include a `if __name__ == "__main__":` entry point and can be executed directly:

1. `src/bronze.py` — create the Bronze staging schema (UNLOGGED, empty)
2. `src/silver.py` — stream raw parquet through Bronze staging and upsert cleaned data into Silver
3. `src/gold.py` — build Gold tables (aggregations + JOIN-based)
4. `src/sql_queries.py` — list available SQL query names
5. `src/main.py` — end-to-end orchestration

## Data Quality Risks

1. Duplicate transactions (same `transaction_id`)
2. Null/empty keys
3. Timestamp formatting inconsistencies

Full details: [docs/report.md](docs/report.md).