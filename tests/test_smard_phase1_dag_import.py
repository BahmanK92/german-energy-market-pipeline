import logging
from pathlib import Path

import pytest

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logger = logging.getLogger(__name__)


def test_smard_phase1_dag_import():
    DagBag = pytest.importorskip("airflow.models").DagBag

    dag_folder = str(Path("airflow/dags").resolve())
    dagbag = DagBag(dag_folder=dag_folder, include_examples=False)

    assert dagbag.import_errors == {}, f"DAG import errors found: {dagbag.import_errors}"

    dag = dagbag.get_dag(dag_id="smard_phase1_pipeline")
    assert dag is not None, "smard_phase1_pipeline DAG was not loaded"

    task_ids = [task.task_id for task in dag.tasks]
    expected_task_ids = [
        "bootstrap_db_task",
        "backfill_smard_task",
        "build_core_task",
        "build_features_task",
        "build_daily_summary_task",
        "run_validations_task",
    ]

    assert task_ids == expected_task_ids, (
        f"Unexpected task IDs. Expected {expected_task_ids}, got {task_ids}"
    )

    print("\nDAG import test passed successfully.")
    print(f"DAG ID: {dag.dag_id}")
    print(f"Task IDs: {task_ids}")


if __name__ == "__main__":
    test_smard_phase1_dag_import()
