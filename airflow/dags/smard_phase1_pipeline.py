from __future__ import annotations

from datetime import datetime, timedelta

# Airflow 3 preferred public DAG authoring API, with Airflow 2 fallback
try:
    from airflow.sdk import dag, task
except ImportError:  # Airflow 2 fallback
    from airflow.decorators import dag, task


@dag(
    dag_id="smard_phase1_pipeline",
    description="Phase 1 German energy market pipeline: bootstrap, backfill, core, features, daily, validations",
    schedule="0 6 * * *",
    start_date=datetime(2026, 4, 1),
    catchup=False,
    default_args={
        "owner": "bahman",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["smard", "phase1", "energy"],
)
def smard_phase1_pipeline():
    @task
    def bootstrap_db_task():
        from scripts.bootstrap_db import bootstrap_db
        bootstrap_db()

    @task
    def backfill_smard_task():
        from scripts.backfill_smard import backfill_smard
        backfill_smard()

    @task
    def build_core_task():
        from scripts.build_core_from_raw import build_and_load_core_from_raw
        build_and_load_core_from_raw()

    @task
    def build_features_task():
        from scripts.build_features_from_core import build_and_load_features_from_core
        build_and_load_features_from_core()

    @task
    def build_daily_summary_task():
        from scripts.build_daily_summary_from_features import (
            build_and_load_daily_summary_from_features,
        )
        build_and_load_daily_summary_from_features()

    @task
    def run_validations_task():
        from scripts.run_phase1_validations import run_phase1_validations
        run_phase1_validations()

    bootstrap = bootstrap_db_task()
    backfill = backfill_smard_task()
    core = build_core_task()
    features = build_features_task()
    daily = build_daily_summary_task()
    validations = run_validations_task()

    bootstrap >> backfill >> core >> features >> daily >> validations


dag = smard_phase1_pipeline()
