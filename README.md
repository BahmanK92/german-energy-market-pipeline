# ⚡ German Energy Market Pipeline (SMARD)

## Overview

This project builds a production-style data pipeline for the German electricity market using SMARD data.

Phase 1 is now complete and includes:

* hourly SMARD ingestion
* normalization into a raw PostgreSQL table
* DB-backed transformation into a core hourly table
* feature engineering for energy-market analysis
* daily summary aggregation
* validation checks
* local execution
* Airflow orchestration

Phase 2 is reserved for analytics and modeling.

---

## Project Status

**Phase 1 execution is complete.**

### Verified:

* local pipeline run works end to end
* smoke check passes
* incremental raw ingestion works
* Airflow runtime works
* Airflow DAG runs successfully
* default backfill scope is limited to the last 2 years

---

## Phase 1 Scope

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
**Storage timezone:** UTC
**Reporting helpers:** Europe/Berlin

---

## Architecture

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

## 🔄 Architecture & Data Flow Explanation

The pipeline follows a layered data engineering design, where each stage has a clear responsibility and builds on the previous one.

### 1. Ingestion (SMARD API → raw layer)

Data is fetched from the SMARD API in batch form (per timestamp and series).
Each batch is normalized into a long-format structure and written into:

* `raw.smard_timeseries_long`

Key characteristics:

* incremental loading (only new batches are fetched)
* idempotent upserts (no duplicates on reruns)
* raw data remains close to source structure

---

### 2. Core Transformation (raw → core)

The raw long-format data is transformed into a wide, analysis-ready hourly table:

* `core.energy_hourly`

What happens here:

* time series are pivoted into columns (e.g. price, load, solar, wind)
* all series are aligned on `datetime_utc`
* Berlin-local time features are derived (`datetime_berlin`, `hour_of_day`, etc.)

This table represents the **clean, unified base dataset**.

---

### 3. Feature Engineering (core → features)

From the core table, domain-specific features are created:

* `mart.energy_features_hourly`

Examples:

* renewable vs fossil generation
* energy shares (renewable_share, fossil_share)
* residual load calculation
* weekday/weekend indicators

This layer prepares the data for **analytics and modeling**.

---

### 4. Aggregation (features → daily)

Hourly data is aggregated into daily metrics:

* `mart.energy_summary_daily`

Examples:

* average / min / max price
* total load estimates
* average renewable and fossil shares

This layer provides **business-level insights**.

---

### 5. Validation

After all transformations, validation checks ensure data quality:

* tables are non-empty
* no duplicate keys
* renewable_share + fossil_share ≈ 1
* residual load calculations are consistent

---

### 6. Orchestration (Airflow)

All steps are orchestrated via the Airflow DAG:

`smard_phase1_pipeline`

Execution order:

```
bootstrap_db
→ backfill_smard
→ build_core
→ build_features
→ build_daily_summary
→ run_validations
```

This ensures the pipeline runs reliably and in the correct order in a production environment.

---

## Output Tables

### raw.smard_timeseries_long

Normalized long-format SMARD observations with idempotent upsert logic.

### core.energy_hourly

Merged hourly base table built from raw data.

### mart.energy_features_hourly

Feature-engineered hourly table with:

* coal
* renewable_generation
* fossil_generation
* total_generation_selected
* renewable_share
* fossil_share
* residual_load
* date/time helper fields

### mart.energy_summary_daily

Berlin-local daily summary table with:

* avg/min/max price
* avg load
* total load estimate
* renewable/fossil aggregates
* residual load metrics
* coal and gas averages
* row_count_hours

---

## What Is Working

Implemented and verified:

* SQL-controlled schemas and tables
* ordered DB bootstrap
* SMARD ingestion into raw layer
* incremental raw loading by latest loaded batch timestamp
* 2-year historical backfill limit by default
* schema-stable loading for core/features/daily
* local end-to-end Phase 1 runner
* validation runner
* smoke check runner
* Airflow DAG orchestration
* test coverage for key Phase 1 flows

---

## Tech Stack

* Python
* pandas
* requests
* SQLAlchemy
* PostgreSQL (Docker)
* Apache Airflow (Docker)
* Docker
* WSL2 / Ubuntu
* VS Code (WSL mode)

---

## Project Structure

```text
german-energy-market-pipeline/
├─ airflow/
│  ├─ dags/
│  ├─ logs/
│  └─ plugins/
├─ config/
├─ sql/
├─ scripts/
├─ src/
│  ├─ clients/
│  ├─ extract/
│  ├─ load/
│  ├─ transform/
│  ├─ utils/
│  └─ viz/
├─ tests/
├─ docs/
├─ README.md
├─ PROJECT_STATUS.md
├─ requirements.txt
├─ requirements-airflow.txt
├─ docker-compose.yml
└─ .env.example
```
---

### Pipeline Evolution

Phase 1 of this project was developed in two stages:

#### 1. Initial (in-memory) pipeline

* Built for rapid development and experimentation
* Runs entirely in Python using pandas
* Entry point: `run_pipeline()` (deprecated for production use)

#### 2. Production pipeline (current)

* Uses PostgreSQL as the source of truth
* Incremental ingestion into `raw` layer
* Downstream tables (`core`, `features`, `daily`) rebuilt from raw
* Orchestrated via Airflow DAG: `smard_phase1_pipeline`

👉 **Important:**
The Airflow DAG and DB-backed scripts represent the production pipeline.
The in-memory pipeline is kept only for development/testing reference.

---

## How to Run Locally

### 1. Start services

```bash
docker compose up -d
```

### 2. Activate the environment

```bash
source venv/bin/activate
```

### 3. Bootstrap the database

```bash
python -m scripts.bootstrap_db
```

### 4. Run the full local Phase 1 pipeline

```bash
python -m scripts.run_phase1_local
```

### 5. Run the smoke check

```bash
python -m scripts.smoke_check_phase1
```

---

## Useful Commands

### Rebuild downstream tables

```bash
python -m scripts.build_core_from_raw
python -m scripts.build_features_from_core
python -m scripts.build_daily_summary_from_features
```

### Run validations

```bash
python -m scripts.run_phase1_validations
```

### Run raw ingestion only

```bash
python -m scripts.backfill_smard
```

### Run key tests

```bash
pytest tests/test_run_phase1_local.py -v
pytest tests/test_smoke_check_phase1.py -v
pytest tests/test_backfill_incremental.py -v
pytest tests/test_smard_phase1_dag_import.py -v
```

---

## Airflow

### Start services

```bash
docker compose up -d
```

### Airflow UI

http://localhost:8080

**Default login:**

* username: airflow
* password: airflow

**DAG:** `smard_phase1_pipeline`

---

## Dependency Notes

* `requirements.txt` → local development
* `requirements-airflow.txt` → Airflow containers

This separation avoids dependency conflicts.

---

## Backfill Rule

The default Phase 1 backfill window is limited to the last 2 years.

---

## Phase 2 Preview

* analytics schema
* regression modeling
* prediction outputs
* evaluation metrics
* dashboard layer

---

## Author

**Bahman Kheradmandi**
