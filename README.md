# Retail ETL on Databricks — Medallion Architecture

An end-to-end batch ETL pipeline built with **PySpark on Databricks**, implementing the
**medallion architecture** (Bronze → Silver → Gold) on Delta Lake, with data-quality
gates and orchestration via **Databricks Workflows**.

```
source CSVs ──▶ BRONZE (raw, Delta) ──▶ SILVER (cleaned, deduped, typed) ──▶ GOLD (aggregated marts)
                        │                          │                                  │
                        └────────── 04_data_quality.py gates ──────────────────────────┘
```

## What it does

| Layer | Notebook | Output |
|---|---|---|
| Bronze | `01_bronze_ingest.py` | Raw CSVs → Delta, plus `_ingest_ts` and `_source_file` lineage columns |
| Silver | `02_silver_clean.py` | Deduplicated, type-cast, validated orders/customers, partitioned by `order_date` |
| Gold | `03_gold_aggregate.py` | `daily_sales_by_category`, `daily_sales_by_region`, `top_products` marts |
| DQ | `04_data_quality.py` | Row-count reconciliation, duplicate, null-rate and total-sales checks — fails the job on critical violations |

## Prerequisites

- A Databricks workspace — the free [Community Edition](https://community.cloud.databricks.com/) works.
- Databricks Runtime 13+ (Delta Lake included).

## Setup

1. Create this repo's files in Databricks Repos (or upload the notebooks).
2. Generate the sample data (run anywhere with Python 3 — laptop or a Databricks notebook):
   ```bash
   python data/generate_retail_data.py --orders 50000 --out ./source
   ```
   This creates `retail_orders.csv` and `retail_customers.csv`.
3. Upload the two CSVs to DBFS, e.g. `/tmp/retail_etl/source/` (Data → Upload, or `dbfs cp`).

## Run it

**Option A — notebooks in order:** open `notebooks/01` → `02` → `03` → `04` and run each.
Each notebook takes widgets `base_path` (default `/tmp/retail_etl`) and `data_path`
(default `/tmp/retail_etl/source`), so no code edits are needed.

**Option B — Databricks Workflow (recommended):** import
`workflows/retail_etl_job.json` via the Jobs API — it chains the four notebooks with
`depends_on`, passing the paths as parameters. The DQ notebook raises on critical
failures, so the job surfaces bad data instead of silently loading it.

```bash
databricks jobs create --json-file workflows/retail_etl_job.json
```

## Data-quality rules enforced

- Bronze row count > 0 (critical)
- Silver rows ≤ Bronze rows — no row multiplication (critical)
- Zero duplicate `order_id` in Silver (critical)
- Null rate < 5% on `order_id`, `customer_id`, `order_date` (warning)
- Gold totals reconcile to Silver totals within 0.1% (critical)

## Extending it

- Swap the CSV source for Auto Loader (`cloudFiles`) to make Bronze streaming.
- Add a `customer_dim` SCD-Type-2 in Silver.
- Point Gold at a BI tool via Databricks SQL.

## Tech

PySpark · Delta Lake · Databricks Workflows · Python (data generator)
