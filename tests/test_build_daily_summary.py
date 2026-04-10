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

client = SmardClient()

series_keys = [
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

normalized_batches = []

for series_key in series_keys:
    timestamps = client.get_available_timestamps(series_key)
    chosen_timestamp = timestamps[-2]   # previous complete batch
    raw_batch = client.get_raw_series(series_key, chosen_timestamp)
    df = normalize_smard_payload(raw_batch)
    normalized_batches.append(df)

core_df = build_core_hourly_table(normalized_batches, how="outer")
features_df = build_features(core_df)
daily_df = build_daily_summary(features_df)

print("\n--- DAILY SUMMARY PREVIEW ---")
print(daily_df.to_string(index=False))

print("\nshape:", daily_df.shape)

print("\nnull counts:")
print(daily_df.isna().sum().to_string())
