# ELT Pipeline Report — Medallion Architecture (Bronze → Silver → Gold)

## 1) Problem Statement (Analytical Goal)

The objective is to build a reproducible ELT pipeline for large transactional data that:

1. Ingests raw transaction records into a database (**Bronze**),
2. Cleans and standardizes those records into an analytics-ready model (**Silver**),
3. Produces curated analytical outputs for business reporting (**Gold**).

Target analytical questions include:

- How does daily transaction volume change over time?
- Which channels contribute the most to transaction count and value?
- Which MCC categories and counterparties drive the largest transaction volume?

---

## 2) Database Architecture

### 2.1 Layers

- **Bronze (raw staging)**  
  PostgreSQL table used as raw ingestion/staging layer. Data is loaded file-by-file and then moved forward.

- **Silver (cleaned core table)**  
  Deduplicated, validated transactional table with upsert logic and `transaction_id` key semantics.

- **Gold (analytics marts)**  
  Aggregated and join-derived reporting tables built from Silver.

### 2.2 Diagrams (Deliverable)

- **ERD:** `docs/diagrams/erd.drawio`
- **High-level architecture:** `docs/diagrams/architecture.drawio`

---

## 3) Implementation Summary (Streaming Bronze → Silver)

To avoid disk exhaustion on very large datasets, the pipeline uses **streaming batch processing**:

1. Read one parquet file from `data/raw/`
2. Load that batch into **Bronze**
3. Upsert cleaned records from Bronze into **Silver**
4. Truncate Bronze
5. Repeat for all files
6. Build Gold tables from Silver

This design ensures peak storage stays close to:

- `Silver (growing)` + `one Bronze batch`

instead of requiring two full-size tables at once.

Both Bronze and Silver are created as **UNLOGGED** PostgreSQL tables to reduce WAL overhead and improve write performance.

---

## 4) ELT Requirement Checklist (Task Compliance)

- ✅ **Initial problem statement (analytical goal)**  
  Defined in this report (Section 1)

- ✅ **Database created**  
  PostgreSQL containerized via `docker-compose.yml`

- ✅ **Raw table loaded**  
  Bronze is actively used as batch staging during Silver build

- ✅ **Cleaned table created**  
  Silver table created and populated with cleaning + deduplication rules

- ✅ **Gold layer with at least 1 JOIN or aggregation query**  
  Gold includes multiple aggregations and a JOIN-based table (`gold_channel_share`)

- ✅ **Reproducible SQL script**  
  `sql/elt_pipeline.sql`

- ✅ **Identified 3 data quality risks**  
  See Section 6

---

## 5) Gold Outputs

The pipeline produces the following Gold tables:

- `gold_daily` — daily KPI rollup
- `gold_by_channel` — metrics by transaction channel
- `gold_by_type` — metrics by transaction type
- `gold_top_counterparties` — top counterparties by volume
- `gold_by_mcc` — top MCC categories
- `gold_channel_share` — JOIN-based channel share metrics

---

## 6) Data Quality Risks (3 Key Risks)

### Risk 1 — Duplicate transaction records

**Issue:** The same `transaction_id` may appear multiple times.  
**Impact:** Inflated volumes and distorted KPIs.  
**Mitigation:** Upsert logic in Silver keeps one canonical record per `transaction_id` (with latest timestamp preference).

### Risk 2 — Missing / empty business keys

**Issue:** `transaction_id` may be null/empty in source data.  
**Impact:** Broken key integrity and unreliable deduplication.  
**Mitigation:** Rows with null/empty `transaction_id` are filtered out before Silver upsert.

### Risk 3 — Timestamp quality / parsing inconsistencies

**Issue:** Source timestamps may vary in quality/format.  
**Impact:** Wrong ordering, dedup conflicts, and incorrect time-based aggregations.  
**Mitigation:** Timestamp normalization/parsing in the Silver flow + typed timestamp columns in the database model.

---

## 7) Reproducibility

### 7.1 End-to-end pipeline

- Start database
- Wait for DB readiness
- Run orchestrator:
  - Bronze schema setup
  - Streaming Bronze → Silver
  - Gold table build

Primary entry point: `src/main.py`  
Convenience runner: `run.sh`

### 7.2 SQL-only reproducibility

A reproducible SQL pipeline is available at:

- `sql/elt_pipeline.sql`

This script recreates medallion objects and Gold outputs in deterministic order.

---

## 8) Repository Deliverables Map

- **Problem Statement:** `docs/report.md` (this file)
- **Architecture (ERD + high-level):** `docs/diagrams/erd.drawio`, `docs/diagrams/architecture.drawio`
- **Versioned code:** `src/`
- **SQL reproducibility:** `sql/elt_pipeline.sql`
- **Data quality risks:** Section 6 in this report

---

## 9) Conclusion

The repository satisfies the assignment goals with a production-practical design for large data volume:

- clear medallion layering,
- reproducible SQL artifacts,
- validated data quality controls,
- and a storage-safe streaming ingestion strategy that successfully scales to the full dataset.
