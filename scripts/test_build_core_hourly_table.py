import logging

from src.clients.smard_client import SmardClient
from src.extract.fetch_timeseries import fetch_latest_timeseries_batch
from src.transform.normalize_smard_json import normalize_smard_payload
from src.transform.build_core_hourly_table import build_core_hourly_table

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

client = SmardClient()

series_keys = ["price", "load", "solar", "wind_onshore", "gas"]

normalized_batches = []

for series_key in series_keys:
    timestamps = client.get_available_timestamps(series_key)
    chosen_timestamp = timestamps[-2]   # use previous batch instead of latest
    raw_batch = client.get_raw_series(series_key, chosen_timestamp)
    df = normalize_smard_payload(raw_batch)
    normalized_batches.append(df)

core_df = build_core_hourly_table(normalized_batches, how="outer")

print("\n--- CORE HOURLY TABLE PREVIEW ---")
print(core_df.head(10).to_string())

print("\nshape:", core_df.shape)
print("\ncolumns:", list(core_df.columns))

print("\nnull counts:")
print(core_df.isna().sum().to_string())

print("\nnon-null counts:")
print(core_df.notna().sum().to_string())

print("\nlast 15 rows:")
print(core_df.tail(15).to_string())
