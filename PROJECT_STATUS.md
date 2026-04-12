# Project Status: German Energy Market Data Pipeline (SMARD)

## Objective

Build a production-style data pipeline for Germany’s electricity market using SMARD data.

Phase 1 goal:

* ingest hourly SMARD data
* store it in a raw PostgreSQL table
* transform into a merged hourly dataset
* build engineered feature tables
* build daily summary tables
* validate the pipeline locally

Phase 2 will focus on analytics and modeling.

---

## Current Environment

* OS: Windows + WSL2 (Ubuntu)
* Development: VS Code (WSL mode)
* Python: virtual environment in WSL
* Database: PostgreSQL (Docker)
* Orchestration: Airflow planned (not yet active)

---

## Current Architecture

```text
SMARD API
   ↓
raw.smard_timeseries_long
   ↓
core.energy_hourly
   ↓
mart.energy_features_hourly
   ↓
mart.energy_summary_daily
```

---

## Current Phase

## Phase 1 — Functional and Validated Locally

The pipeline is working end-to-end.

Implemented:

* SMARD API client
* timestamp index fetching
* timeseries extraction
* JSON normalization
* raw upsert into PostgreSQL
* core table build from raw
* feature engineering from core
* daily summary build from features
* validation runner
* smoke check runner
* local Phase 1 pipeline runner

---

## Data Scope

### Market and demand

* price (DE-LU)
* load (DE)
* residual_load_smard (DE)

### Renewable generation

* solar
* wind_onshore
* wind_offshore
* biomass
* hydro

### Conventional generation

* lignite
* hard_coal
* gas
* other_conventional

Resolution: hourly
Storage: UTC
Reporting helpers: Berlin-local time fields

---

## Tables

### raw.smard_timeseries_long

Normalized raw ingestion table.

### core.energy_hourly

Merged hourly base dataset.

### mart.energy_features_hourly

Feature-engineered hourly dataset.

### mart.energy_summary_daily

Berlin-local daily aggregation dataset.

---

## Completed

* [x] PostgreSQL setup (Docker)
* [x] Python environment (WSL)
* [x] Project structure
* [x] Config files
* [x] SMARD API client
* [x] Raw table schema
* [x] Core and mart schemas
* [x] Ordered DB bootstrap
* [x] Idempotent raw upsert
* [x] Core table build
* [x] Feature table build
* [x] Daily summary build
* [x] Schema-stable loading (truncate + append)
* [x] Validation utilities
* [x] Local Phase 1 runner
* [x] Validation runner script
* [x] Smoke check script
* [x] Incremental ingestion by latest batch timestamp

---

## Validation Status

All validations passing:

* tables are non-empty
* renewable_share + fossil_share ≈ 1
* residual load comparison is stable
* reruns are idempotent
* full pipeline run succeeds

---

## Immediate Next Steps

1. Finalize documentation
2. Optional: add visualizations or screenshots
3. Decide on Airflow integration
4. If yes → build DAG after pipeline is frozen

---

## Notes

* Raw ingestion is incremental and idempotent
* Downstream tables are rebuilt cleanly from raw
* PostgreSQL runs in Docker
* Airflow is not yet part of the runtime pipeline

---

## Phase 2 Preview

* regression modeling
* price prediction
* evaluation metrics
* analytical dashboards
