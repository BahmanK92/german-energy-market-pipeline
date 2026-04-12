import logging

from sqlalchemy import text

from scripts.run_phase1_local import run_phase1_local
from src.load.postgres import get_engine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logger = logging.getLogger(__name__)


def test_run_phase1_local():
    engine = get_engine()

    run_phase1_local()

    queries = {
        "raw_count": "SELECT COUNT(*) FROM raw.smard_timeseries_long",
        "core_count": "SELECT COUNT(*) FROM core.energy_hourly",
        "features_count": "SELECT COUNT(*) FROM mart.energy_features_hourly",
        "daily_count": "SELECT COUNT(*) FROM mart.energy_summary_daily",
    }

    results = {}

    with engine.connect() as conn:
        for key, query in queries.items():
            results[key] = conn.execute(text(query)).scalar()

    assert results["raw_count"] > 0, "raw.smard_timeseries_long should not be empty"
    assert results["core_count"] > 0, "core.energy_hourly should not be empty"
    assert results["features_count"] > 0, "mart.energy_features_hourly should not be empty"
    assert results["daily_count"] > 0, "mart.energy_summary_daily should not be empty"

    print("\nrun_phase1_local test passed successfully.")
    print(results)


if __name__ == "__main__":
    test_run_phase1_local()