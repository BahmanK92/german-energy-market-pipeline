# ⚡ German Energy Market Pipeline (SMARD)

## 📌 Overview

This project builds a **production-style data pipeline** for the German electricity market using data from the SMARD API.

The pipeline ingests, processes, and aggregates hourly energy data to enable analysis of:

* electricity prices
* demand (load)
* renewable vs fossil generation
* residual load dynamics

---

## 🎯 Objectives

* Build a **real-world data engineering pipeline**
* Work with **live, incremental data**
* Apply **feature engineering based on energy domain knowledge**
* Prepare data for **future modeling and prediction (Phase 2)**

---

## 🏗️ Architecture

```text
SMARD API
   ↓
Extraction (Python)
   ↓
Normalization (Pandas)
   ↓
Core Hourly Table
   ↓
Feature Engineering
   ↓
Daily Summary Tables
   ↓
PostgreSQL (Docker)
   ↓
Airflow (planned)
```

---

## ⚙️ Tech Stack

* **Python** (pandas, requests, sqlalchemy)
* **PostgreSQL** (Dockerized)
* **Docker** (local infrastructure)
* **Airflow** (planned orchestration)
* **Git & GitHub**
* **WSL (Linux environment on Windows)**

---

## 📊 Data Sources

* SMARD API (German electricity market data)

Main data categories:

* price (DE-LU)
* load (DE)
* renewable generation (solar, wind, biomass, hydro)
* fossil generation (lignite, coal, gas)
* residual load

---

## 🔧 Pipeline Components (Phase 1)

### Ingestion

* Fetch available timestamps
* Retrieve raw SMARD time series data

### Transformation

* Normalize raw JSON → structured DataFrame
* Merge all series into a unified hourly dataset

### Feature Engineering

* Renewable vs fossil generation
* Coal aggregation (lignite + hard coal)
* Residual load calculation
* Time-based features

### Aggregation

* Daily summary tables:

  * price statistics
  * generation mix
  * residual load metrics

---

## 🗄️ Data Model

### Tables

* `raw.smard_timeseries_long` → normalized raw data
* `core.energy_hourly` → merged hourly dataset
* `mart.energy_features_hourly` → engineered features
* `mart.energy_summary_daily` → daily aggregates

---

## 🚀 Current Status

✅ Phase 1 (Data Engineering) — **In Progress / Functional**

* End-to-end local pipeline working
* SMARD data ingestion implemented
* Feature engineering completed
* Daily aggregation completed
* PostgreSQL integration working

---

## 🔮 Next Steps (Phase 2)

* Regression modeling (price vs residual load)
* Prediction of electricity prices
* Model evaluation
* Extended analytical insights

---

## 🧠 Key Concepts

* Residual Load Analysis
* Renewable vs Fossil Energy Mix
* Time Series Feature Engineering
* Incremental Data Pipelines

---

## ▶️ How to Run (Simplified)

```bash
# activate environment
source venv/bin/activate

# run local pipeline
python -m scripts.run_phase1_local
```

---

## 💡 Why This Project

This project demonstrates:

* practical data engineering skills
* working with real-world energy data
* building reproducible pipelines
* structuring data for analytics and modeling

---

## 📬 Author

Bahman Kheradmandi