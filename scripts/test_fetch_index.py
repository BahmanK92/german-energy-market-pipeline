import logging
import json

from src.clients.smard_client import SmardClient
from src.extract.fetch_index import fetch_series_index, fetch_many_series_indexes

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

client = SmardClient()

print("\n--- ONE SERIES TEST ---")
price_index = fetch_series_index(client, "price")
preview = {
    "series_key": price_index["series_key"],
    "n_timestamps": price_index["n_timestamps"],
    "latest_timestamp": price_index["latest_timestamp"],
    "first_3_timestamps": price_index["available_timestamps"][:3],
    "last_3_timestamps": price_index["available_timestamps"][-3:],
}
print(json.dumps(preview, indent=2))

print("\n--- MULTI SERIES TEST ---")
results = fetch_many_series_indexes(client, ["price", "load", "solar"])

for item in results:
    print(
        f"{item['series_key']}: "
        f"{item['n_timestamps']} timestamps | "
        f"latest={item['latest_timestamp']}"
    )
    