import logging
import json

from src.clients.smard_client import SmardClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

client = SmardClient()

timestamps = client.get_available_timestamps("price")
print(f"Found {len(timestamps)} timestamps")
print("Last 5 timestamps:", timestamps[-5:])

latest = client.get_latest_raw_series("price")
print("Keys in result:", latest.keys())
print("Series key:", latest["series_key"])
print("Batch timestamp:", latest["source_batch_timestamp"])

# Only preview a bit of payload structure
print(json.dumps(latest["payload"], indent=2)[:1000])