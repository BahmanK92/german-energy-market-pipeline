import logging

from src.clients.smard_client import SmardClient
from src.transform.normalize_smard_json import normalize_smard_payload
from src.transform.build_core_hourly_table import build_core_hourly_table
from src.transform.build_features import build_features
from src.transform.build_daily_summary import build_daily_summary

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logger = logging.getLogger(__name__)


PHASE1_SERIES_KEYS = [
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
    This avoids partial future/incomplete data.
    """
    timestamps = client.get_available_timestamps(series_key)

    if len(timestamps) < 2:
        raise ValueError(f"Not enough timestamps available for series '{series_key}'")

    return timestamps[-2]


def run_phase1_local() -> tuple:
    logger.info("Starting local Phase 1 pipeline run")

    client = SmardClient()
    normalized_batches = []

    for series_key in PHASE1_SERIES_KEYS:
        chosen_timestamp = choose_complete_batch_timestamp(client, series_key)
        logger.info(
            "Using batch timestamp %s for series '%s'",
            chosen_timestamp,
            series_key,
        )

        raw_batch = client.get_raw_series(series_key, chosen_timestamp)
        normalized_df = normalize_smard_payload(raw_batch)
        normalized_batches.append(normalized_df)

    core_df = build_core_hourly_table(normalized_batches, how="outer")
    features_df = build_features(core_df)
    daily_df = build_daily_summary(features_df)

    logger.info("Phase 1 local run finished successfully")
    return core_df, features_df, daily_df


if __name__ == "__main__":
    core_df, features_df, daily_df = run_phase1_local()

    print("\n=== CORE TABLE ===")
    print(core_df.head(5).to_string())
    print("\ncore shape:", core_df.shape)

    print("\n=== FEATURES TABLE ===")
    feature_preview_cols = [
        "datetime_berlin",
        "load_mw",
        "renewable_generation_mw",
        "fossil_generation_mw",
        "renewable_share",
        "fossil_share",
        "residual_load_mw",
        "day_of_week",
        "is_weekend",
    ]
    print(features_df[feature_preview_cols].head(5).to_string())
    print("\nfeatures shape:", features_df.shape)

    print("\n=== DAILY SUMMARY ===")
    print(daily_df.to_string(index=False))
    print("\ndaily shape:", daily_df.shape)
    