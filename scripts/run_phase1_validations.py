import logging

import pandas as pd

from src.load.postgres import get_engine
from src.utils.validation import run_phase1_dataframe_validations

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logger = logging.getLogger(__name__)


def run_phase1_validations() -> None:
    engine = get_engine()

    logger.info("Reading raw.smard_timeseries_long ...")
    raw_df = pd.read_sql_table("smard_timeseries_long", con=engine, schema="raw")

    logger.info("Reading core.energy_hourly ...")
    core_df = pd.read_sql_table("energy_hourly", con=engine, schema="core")

    logger.info("Reading mart.energy_features_hourly ...")
    features_df = pd.read_sql_table("energy_features_hourly", con=engine, schema="mart")

    logger.info("Reading mart.energy_summary_daily ...")
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

    logger.info("Phase 1 validations completed successfully.")


if __name__ == "__main__":
    run_phase1_validations()