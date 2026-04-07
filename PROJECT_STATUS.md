# Project: German Energy Market Data Pipeline (SMARD)

## 📌 Objective

Build a production-style data pipeline for Germany’s electricity market using SMARD data.
The pipeline will ingest, clean, transform, and aggregate hourly energy data into structured tables for analysis.

---

## ⚙️ Environment Setup

* OS: Windows + WSL2 (Ubuntu)
* Development: Visual Studio Code (WSL mode)
* Python: Python 3 (WSL) with virtual environment
* Containers: Docker Desktop (WSL backend)
* Orchestration: Apache Airflow (Docker)
* Database: PostgreSQL (Docker)

---

## 📁 Project Structure (Initialized)

```
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
├─ requirements.txt
├─ docker-compose.yml
├─ .env.example
```

---

## 🚧 Current Phase

**Phase 1 — Data Engineering Pipeline**

Scope:

* Ingestion (SMARD API)
* Cleaning and normalization
* Feature engineering (energy domain features)
* Daily aggregation tables
* Batch pipeline (local + Airflow)

---

## 📊 Data Scope (Phase 1)

### Market & Demand

* price (DE-LU)
* load (DE)
* residual_load_smard (DE)

### Renewable Generation

* solar
* wind_onshore
* wind_offshore
* biomass
* hydro

### Conventional Generation

* lignite
* hard_coal
* gas
* other_conventional

Resolution: Hourly
Timezone: Europe/Berlin (with UTC storage)

---

## 🧠 Planned Feature Engineering

* coal = lignite + hard_coal
* renewable_generation
* fossil_generation
* total_generation_selected
* renewable_share
* fossil_share
* residual_load (calculated)
* time features:

  * date_berlin
  * hour_of_day
  * day_of_week
  * is_weekend

---

## 🗄️ Planned Tables

### raw.smard_timeseries_long

Normalized API data (long format)

### core.energy_hourly

Merged hourly dataset (clean base table)

### mart.energy_features_hourly

Feature-engineered hourly dataset

### mart.energy_summary_daily

Daily aggregated metrics

---

## ✅ Completed

* [x] WSL2 + Ubuntu setup
* [x] Docker Desktop installed and configured
* [x] Airflow container running
* [x] PostgreSQL container running
* [x] Python environment (venv) created in WSL
* [x] Core project folder structure initialized
* [x] VS Code configured in WSL mode
* [x] requirements.txt created

---

## 🔄 In Progress

* [ ] Config files (`smard_filters.yml`, `settings.py`)
* [ ] SMARD API client (`smard_client.py`)

---

## ⏭️ Next Steps (Immediate)

1. Create `config/smard_filters.yml`
2. Create `config/settings.py`
3. Convert notebook logic → `src/clients/smard_client.py`
4. Test single series data fetch
5. Begin extraction module (`fetch_index.py`)

---

## 📌 Notes

* Initial development will use a small subset of data (e.g., last 60 days)
* Pipeline will be tested locally before Airflow integration
* Notebook logic will be gradually converted into modular Python scripts
* Database will be the main output (CSV export optional)

---

## 🚀 Future (Phase 2 Preview)

* Regression modeling (price vs residual load, renewables)
* Predictions and evaluation
* Additional analysis modules

---
