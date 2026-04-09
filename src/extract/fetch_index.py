import logging
from typing import Dict, List, Optional

from src.clients.smard_client import SmardClient

logger = logging.getLogger(__name__)


def fetch_series_index(client: SmardClient, series_key: str) -> Dict:
    """
    Fetch available batch timestamps for one SMARD series
    and return clean metadata for downstream pipeline steps.
    """
    timestamps = client.get_available_timestamps(series_key)

    result = {
        "series_key": series_key,
        "available_timestamps": timestamps,
        "latest_timestamp": timestamps[-1] if timestamps else None,
        "n_timestamps": len(timestamps),
    }

    logger.info(
        "Fetched index for series '%s' with %s timestamps",
        series_key,
        result["n_timestamps"],
    )
    return result


def fetch_many_series_indexes(
    client: SmardClient,
    series_keys: Optional[List[str]] = None,
) -> List[Dict]:
    """
    Fetch index metadata for multiple series.

    If series_keys is None, fetch all configured series from the client config.
    """
    if series_keys is None:
        series_keys = list(client.series_config.keys())

    results = []
    for series_key in series_keys:
        results.append(fetch_series_index(client, series_key))

    logger.info("Fetched index metadata for %s series", len(results))
    return results
    