import logging

import pandas as pd

from src.load.postgres import get_engine
from src.transform.build_features import build_features

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logger = logging.getLogger(__name__)


def build_and_load_features_from_core() -> None:
    engine = get_engine()

    logger.info("Reading core.energy_hourly ...")
    core_df = pd.read_sql_table("energy_hourly", con=engine, schema="core")

    core_df["datetime_utc"] = pd.to_datetime(core_df["datetime_utc"], utc=True)
    core_df["datetime_berlin"] = pd.to_datetime(core_df["datetime_berlin"])

    features_df = build_features(core_df)

    logger.info("Writing mart.energy_features_hourly ...")
    features_df.to_sql(
        name="energy_features_hourly",
        con=engine,
        schema="mart",
        if_exists="replace",
        index=False,
        method="multi",
    )

    logger.info(
        "mart.energy_features_hourly written successfully with %s rows and %s columns",
        len(features_df),
        len(features_df.columns),
    )


if __name__ == "__main__":
    build_and_load_features_from_core()