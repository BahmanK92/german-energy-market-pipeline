import logging

from src.clients.smard_client import SmardClient
from src.extract.fetch_timeseries import fetch_one_timeseries_batch
from src.transform.normalize_smard_json import normalize_smard_payload
from src.transform.build_core_hourly_table import build_core_hourly_table
from src.transform.build_features import build_features
from src.transform.build_daily_summary import build_daily_summary
from src.load.postgres import get_engine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logger = logging.getLogger(__name__)


SERIES = [
    "price",
    "load",
    "residual_load_smard",
    "solar",
    "wind_onshore",
    "wind_offshore",
    "biomass",
    "hydro",
    "lignite",
    "hard_coal",
    "gas",
    "other_conventional",
]


def choose_complete_batch_timestamp(client: SmardClient, series_key: str) -> int:
    """
    For local development, use the previous batch instead of the newest open batch.
    This avoids partial or still-open/incomplete data.
    """
    timestamps = client.get_available_timestamps(series_key)

    if len(timestamps) < 2:
        raise ValueError(f"Not enough timestamps available for series '{series_key}'")

    return timestamps[-2]


def run_pipeline():
    """
    Run the Phase 1 pipeline in memory and return the three output DataFrames.
    """
    logger.info("Starting pipeline run")

    client = SmardClient()
    normalized_dfs = []

    for series_key in SERIES:
        batch_timestamp = choose_complete_batch_timestamp(client, series_key)

        logger.info(
            "Processing series '%s' with batch timestamp %s",
            series_key,
            batch_timestamp,
        )

        raw_batch = fetch_one_timeseries_batch(
            client=client,
            series_key=series_key,
            timestamp=batch_timestamp,
        )

        df = normalize_smard_payload(raw_batch)
        normalized_dfs.append(df)

    core_df = build_core_hourly_table(normalized_dfs, how="outer")
    features_df = build_features(core_df)
    daily_df = build_daily_summary(features_df)

    logger.info(
        "Pipeline run completed: core=%s rows, features=%s rows, daily=%s rows",
        len(core_df),
        len(features_df),
        len(daily_df),
    )

    return core_df, features_df, daily_df


def load_to_postgres(core_df, features_df, daily_df):
    """
    Write the pipeline outputs into PostgreSQL.
    """
    engine = get_engine()

    logger.info("Writing core.energy_hourly ...")
    core_df.to_sql(
        name="energy_hourly",
        con=engine,
        schema="core",
        if_exists="replace",
        index=False,
        method="multi",
    )

    logger.info("Writing mart.energy_features_hourly ...")
    features_df.to_sql(
        name="energy_features_hourly",
        con=engine,
        schema="mart",
        if_exists="replace",
        index=False,
        method="multi",
    )

    logger.info("Writing mart.energy_summary_daily ...")
    daily_df.to_sql(
        name="energy_summary_daily",
        con=engine,
        schema="mart",
        if_exists="replace",
        index=False,
        method="multi",
    )

    logger.info("All tables written successfully")


if __name__ == "__main__":
    logger.info("Starting full pipeline + load to Postgres")

    core_df, features_df, daily_df = run_pipeline()
    load_to_postgres(core_df, features_df, daily_df)

    print("Data successfully loaded into Postgres")
    