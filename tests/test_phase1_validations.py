import logging

import pandas as pd

from src.load.postgres import get_engine
from src.utils.validation import run_phase1_dataframe_validations

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logger = logging.getLogger(__name__)


def test_phase1_validations():
    engine = get_engine()

    raw_df = pd.read_sql_table("smard_timeseries_long", con=engine, schema="raw")
    core_df = pd.read_sql_table("energy_hourly", con=engine, schema="core")
    features_df = pd.read_sql_table("energy_features_hourly", con=engine, schema="mart")
    daily_df = pd.read_sql_table("energy_summary_daily", con=engine, schema="mart")

    raw_df["datetime_utc"] = pd.to_datetime(raw_df["datetime_utc"], utc=True)
    core_df["datetime_utc"] = pd.to_datetime(core_df["datetime_utc"], utc=True)
    core_df["datetime_berlin"] = pd.to_datetime(core_df["datetime_berlin"])
    features_df["datetime_utc"] = pd.to_datetime(features_df["datetime_utc"], utc=True)
    features_df["datetime_berlin"] = pd.to_datetime(features_df["datetime_berlin"])

    run_phase1_dataframe_validations(
        raw_df=raw_df,
        core_df=core_df,
        features_df=features_df,
        daily_df=daily_df,
    )

    print("\nPhase 1 validations passed successfully.")


if __name__ == "__main__":
    test_phase1_validations()