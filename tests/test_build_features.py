import logging

from src.clients.smard_client import SmardClient
from src.transform.normalize_smard_json import normalize_smard_payload
from src.transform.build_core_hourly_table import build_core_hourly_table
from src.transform.build_features import build_features

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

client = SmardClient()

# For cleaner testing, use previous complete batch
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
    chosen_timestamp = timestamps[-2]
    raw_batch = client.get_raw_series(series_key, chosen_timestamp)
    df = normalize_smard_payload(raw_batch)
    normalized_batches.append(df)

core_df = build_core_hourly_table(normalized_batches, how="outer")
features_df = build_features(core_df)

print("\n--- FEATURES TABLE PREVIEW ---")
preview_cols = [
    "datetime_berlin",
    "date_berlin",
    "hour_of_day",
    "load_mw",
    "residual_load_smard_mw",
    "solar_mw",
    "wind_onshore_mw",
    "wind_offshore_mw",
    "biomass_mw",
    "hydro_mw",
    "lignite_mw",
    "hard_coal_mw",
    "gas_mw",
    "other_conventional_mw",
    "coal_mw",
    "renewable_generation_mw",
    "fossil_generation_mw",
    "total_generation_selected_mw",
    "renewable_share",
    "fossil_share",
    "residual_load_mw",
    "day_of_week",
    "is_weekend",
]
print(features_df[preview_cols].head(10).to_string())

print("\nshape:", features_df.shape)

print("\nnull counts for engineered columns:")
engineered_cols = [
    "coal_mw",
    "renewable_generation_mw",
    "fossil_generation_mw",
    "total_generation_selected_mw",
    "renewable_share",
    "fossil_share",
    "residual_load_mw",
    "day_of_week",
    "is_weekend",
]
print(features_df[engineered_cols].isna().sum().to_string())
