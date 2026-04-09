import logging
import json

from src.clients.smard_client import SmardClient
from src.extract.fetch_timeseries import (
    fetch_one_timeseries_batch,
    fetch_latest_timeseries_batch,
    fetch_many_timeseries_batches,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

client = SmardClient()

print("\n--- TEST 1: FETCH LATEST PRICE BATCH ---")
latest_price = fetch_latest_timeseries_batch(client, "price")
print("series_key:", latest_price["series_key"])
print("batch_timestamp:", latest_price["source_batch_timestamp"])
print("payload keys:", list(latest_price["payload"].keys()))
print("first 3 rows from payload series:")
print(json.dumps(latest_price["payload"]["series"][:3], indent=2))

print("\n--- TEST 2: FETCH ONE SPECIFIC LOAD BATCH ---")
load_timestamps = client.get_available_timestamps("load")
one_load = fetch_one_timeseries_batch(client, "load", load_timestamps[-1])
print("series_key:", one_load["series_key"])
print("batch_timestamp:", one_load["source_batch_timestamp"])
print("first 3 rows from payload series:")
print(json.dumps(one_load["payload"]["series"][:3], indent=2))

print("\n--- TEST 3: FETCH LAST 2 SOLAR BATCHES ---")
solar_batches = fetch_many_timeseries_batches(client, "solar", limit=2)
for i, batch in enumerate(solar_batches, start=1):
    print(
        f"batch {i}: "
        f"series_key={batch['series_key']} | "
        f"timestamp={batch['source_batch_timestamp']} | "
        f"n_rows={len(batch['payload'].get('series', []))}"
    )
    