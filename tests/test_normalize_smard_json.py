import logging

from src.clients.smard_client import SmardClient
from src.extract.fetch_timeseries import fetch_latest_timeseries_batch
from src.transform.normalize_smard_json import normalize_smard_payload

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

client = SmardClient()

print("\n--- TEST 1: NORMALIZE PRICE ---")
raw_price = fetch_latest_timeseries_batch(client, "price")
df_price = normalize_smard_payload(raw_price)

print(df_price.head(5).to_string())
print("\nshape:", df_price.shape)
print("\ncolumns:", list(df_price.columns))

print("\n--- TEST 2: NORMALIZE LOAD ---")
raw_load = fetch_latest_timeseries_batch(client, "load")
df_load = normalize_smard_payload(raw_load)

print(df_load.head(5).to_string())
print("\nshape:", df_load.shape)
print("\ncolumns:", list(df_load.columns))
