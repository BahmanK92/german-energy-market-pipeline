import logging

import pandas as pd

from src.load.postgres import get_engine, truncate_table
from src.transform.build_daily_summary import build_daily_summary

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logger = logging.getLogger(__name__)


def build_and_load_daily_summary_from_features() -> None:
    engine = get_engine()

    logger.info("Reading mart.energy_features_hourly ...")
    features_df = pd.read_sql_table("energy_features_hourly", con=engine, schema="mart")

    features_df["datetime_utc"] = pd.to_datetime(features_df["datetime_utc"], utc=True)
    features_df["datetime_berlin"] = pd.to_datetime(features_df["datetime_berlin"])
    features_df["date_berlin"] = pd.to_datetime(features_df["date_berlin"]).dt.date

    daily_df = build_daily_summary(features_df)

    logger.info("Truncating mart.energy_summary_daily ...")
    truncate_table(engine=engine, table_name="energy_summary_daily", schema="mart")

    logger.info("Appending rows into mart.energy_summary_daily ...")
    daily_df.to_sql(
        name="energy_summary_daily",
        con=engine,
        schema="mart",
        if_exists="append",
        index=False,
        method="multi",
    )

    logger.info(
        "mart.energy_summary_daily written successfully with %s rows and %s columns",
        len(daily_df),
        len(daily_df.columns),
    )


if __name__ == "__main__":
    build_and_load_daily_summary_from_features()