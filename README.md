# ⚡ German Energy Market Pipeline (SMARD)

## Overview

This project builds a production-style data pipeline for the German electricity market using SMARD data.

Phase 1 focuses on:

* hourly SMARD ingestion
* normalization into a raw table
* DB-backed transformation into a core hourly table
* feature engineering for energy-market analysis
* daily summary aggregation
* local pipeline execution with validations

Phase 2 is reserved for analytics and modeling later.

---

## Phase 1 Scope

The current Phase 1 pipeline covers these SMARD series:

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
Storage timezone: UTC
Reporting helpers: Europe/Berlin

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

## Current Phase 1 Outputs

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
* date/time features

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

## What Is Working Now

Phase 1 is functional and DB-backed.

Implemented and validated:

* SQL-controlled schemas and tables
* ordered DB bootstrap
* SMARD ingestion into raw layer
* incremental raw loading by latest loaded batch timestamp
* schema-stable loading for core/features/daily
* local end-to-end Phase 1 runner
* validation runner
* smoke check runner
* test coverage for bootstrap, core/features/daily stability, validations, and local runner

---

## Tech Stack

* Python
* pandas
* requests
* SQLAlchemy
* PostgreSQL (Docker)
* Docker
* WSL2 / Ubuntu
* VS Code (WSL mode)

Note: PostgreSQL is currently running in Docker. Airflow orchestration is planned later.

---

## Project Structure

```text
german-energy-market-pipeline/
├─ airflow/
│  └─ dags/
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
├─ docker-compose.yml
└─ .env.example
```

---

## How to Run

### 1. Start PostgreSQL

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

### 4. Run the local Phase 1 pipeline

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

### Run end-to-end test

```bash
python -m tests.test_run_phase1_local
```

---

## Current Status

Phase 1 is working locally with:

* incremental raw ingestion
* DB-backed transformations
* validation checks
* schema-stable loads
* daily summary outputs

Remaining Phase 1 polish:

* documentation refinement
* optional Airflow DAG
* optional visualization layer

---

## Why This Project

This project demonstrates:

* practical data engineering with real-world data
* warehouse-style pipeline design
* incremental ingestion patterns
* reproducible local execution
* preparation for analytics and modeling

---

## Author

Bahman Kheradmandi
