# Project Status: German Energy Market Data Pipeline (SMARD)

## Objective

Build a production-style data pipeline for Germany’s electricity market using SMARD data.

### Phase 1 goal:

* ingest hourly SMARD data
* store it in a raw PostgreSQL table
* transform into a merged hourly dataset
* build engineered feature tables
* build daily summary tables
* validate the pipeline locally
* run the pipeline through Airflow orchestration

Phase 2 will focus on analytics and modeling.

---

## Current Environment

* OS: Windows + WSL2 (Ubuntu)
* Development: VS Code (WSL mode)
* Python: virtual environment in WSL
* Database: PostgreSQL (Docker)
* Orchestration: Apache Airflow (Docker)

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

**Phase 1 — Execution Complete**

The pipeline is working end-to-end locally and through Airflow.

---

## Implemented

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
* Airflow DAG orchestration

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

**Resolution:** hourly
**Storage:** UTC
**Reporting helpers:** Berlin-local time fields

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

* PostgreSQL setup (Docker)
* Python environment (WSL)
* Project structure
* Config files
* SMARD API client
* Raw table schema
* Core and mart schemas
* Ordered DB bootstrap
* Idempotent raw upsert
* Core table build
* Feature table build
* Daily summary build
* Schema-stable loading (truncate + append)
* Validation utilities
* Local Phase 1 runner
* Validation runner script
* Smoke check script
* Incremental ingestion by latest batch timestamp
* Default backfill limited to last 2 years
* Airflow runtime setup
* Airflow DAG visible in UI
* Airflow DAG run successful

---

## Validation Status

All key Phase 1 validations are passing:

* tables are non-empty
* renewable_share + fossil_share ≈ 1
* residual load comparison is stable
* reruns are idempotent
* full pipeline run succeeds
* Airflow DAG tasks succeed

---

## Current Output State

Latest verified state:

* raw table populated
* core hourly table populated
* feature table populated
* daily summary table populated
* latest timestamps aligned
* Airflow orchestration successful

---

## Notes

* Raw ingestion is incremental and idempotent
* Downstream tables are rebuilt cleanly from raw
* Default historical backfill is limited to the last 2 years
* PostgreSQL runs in Docker
* Airflow runs in Docker
* `requirements-airflow.txt` is separated from local `requirements.txt` to avoid dependency conflicts

---

## Phase 2 Preview

* analytics schema
* regression modeling
* price prediction
* evaluation metrics
* analytical dashboards
