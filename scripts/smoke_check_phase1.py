import logging

from sqlalchemy import text

from src.load.postgres import get_engine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logger = logging.getLogger(__name__)


def smoke_check_phase1() -> None:
    engine = get_engine()

    queries = {
        "raw_count": "SELECT COUNT(*) FROM raw.smard_timeseries_long",
        "core_count": "SELECT COUNT(*) FROM core.energy_hourly",
        "features_count": "SELECT COUNT(*) FROM mart.energy_features_hourly",
        "daily_count": "SELECT COUNT(*) FROM mart.energy_summary_daily",
        "raw_max_datetime_utc": "SELECT MAX(datetime_utc) FROM raw.smard_timeseries_long",
        "core_max_datetime_utc": "SELECT MAX(datetime_utc) FROM core.energy_hourly",
        "daily_max_date_berlin": "SELECT MAX(date_berlin) FROM mart.energy_summary_daily",
    }

    results = {}

    with engine.connect() as conn:
        for key, query in queries.items():
            results[key] = conn.execute(text(query)).scalar()

    assert results["raw_count"] > 0, "raw.smard_timeseries_long is empty"
    assert results["core_count"] > 0, "core.energy_hourly is empty"
    assert results["features_count"] > 0, "mart.energy_features_hourly is empty"
    assert results["daily_count"] > 0, "mart.energy_summary_daily is empty"

    print("\nPhase 1 smoke check passed successfully.")
    print(f"raw rows: {results['raw_count']}")
    print(f"core rows: {results['core_count']}")
    print(f"features rows: {results['features_count']}")
    print(f"daily rows: {results['daily_count']}")
    print(f"latest raw datetime_utc: {results['raw_max_datetime_utc']}")
    print(f"latest core datetime_utc: {results['core_max_datetime_utc']}")
    print(f"latest daily date_berlin: {results['daily_max_date_berlin']}")


if __name__ == "__main__":
    smoke_check_phase1()