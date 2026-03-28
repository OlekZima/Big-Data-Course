# Database Architecture

## Layers

- **Bronze**  
  PostgreSQL table used as raw ingestion/staging layer. Data is loaded file-by-file and then moved forward.

- **Silver**  
  Deduplicated, validated transactional table with upsert logic and `transaction_id` key semantics.

- **Gold**  
  Aggregated and join-derived reporting tables built from Silver.

---

## Streaming Bronze to Silver

To avoid disk exhaustion, the pipeline uses **streaming batch processing**:

1. Read one parquet file from `data/raw/`
2. Load that batch into **Bronze**
3. Upsert cleaned records from Bronze into **Silver**
4. Truncate Bronze
5. Repeat for all files
6. Build Gold tables from Silver

Both Bronze and Silver are created as **UNLOGGED** PostgreSQL tables to reduce WAL overhead.

---

## Gold Outputs

The pipeline produces the following Gold tables:

- `gold_daily` — daily KPI rollup
- `gold_by_channel` — metrics by transaction channel
- `gold_by_type` — metrics by transaction type
- `gold_top_counterparties` — top counterparties by volume
- `gold_by_mcc` — top MCC categories
- `gold_channel_share` — JOIN-based channel share metrics

---

## Data Quality Risks

### Risk 1 — Duplicate transaction records

**Issue:** The same `transaction_id` may appear multiple times.  
**Mitigation:** Upsert logic in Silver keeps one canonical record per `transaction_id` (with latest timestamp preference).

### Risk 2 — Missing / empty business keys

**Issue:** `transaction_id` may be null/empty in source data.  
**Mitigation:** Rows with null/empty `transaction_id` are filtered out before Silver upsert.

### Risk 3 — Timestamp quality / parsing inconsistencies

**Issue:** Source timestamps may vary in quality/format.  
**Mitigation:** Timestamp normalization/parsing in the Silver flow + typed timestamp columns in the database model.
