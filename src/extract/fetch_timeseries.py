import logging
from typing import Dict, List, Optional

from src.clients.smard_client import SmardClient

logger = logging.getLogger(__name__)


def fetch_one_timeseries_batch(
    client: SmardClient,
    series_key: str,
    timestamp: int,
) -> Dict:
    """
    Fetch one raw SMARD payload for a given series and batch timestamp.
    """
    result = client.get_raw_series(series_key=series_key, timestamp=timestamp)

    logger.info(
        "Fetched raw timeseries batch for series '%s' at timestamp %s",
        series_key,
        timestamp,
    )
    return result


def fetch_latest_timeseries_batch(
    client: SmardClient,
    series_key: str,
) -> Dict:
    """
    Fetch the latest available raw SMARD payload for one series.
    """
    result = client.get_latest_raw_series(series_key)

    logger.info(
        "Fetched latest raw timeseries batch for series '%s'",
        series_key,
    )
    return result


def fetch_many_timeseries_batches(
    client: SmardClient,
    series_key: str,
    timestamps: Optional[List[int]] = None,
    limit: Optional[int] = None,
) -> List[Dict]:
    """
    Fetch multiple raw SMARD payloads for one series.

    Rules:
    - if timestamps is provided, use it
    - otherwise fetch all available timestamps from index
    - if limit is provided, only keep the latest N timestamps
    """
    if timestamps is None:
        timestamps = client.get_available_timestamps(series_key)

    timestamps = sorted(set(int(ts) for ts in timestamps))

    if limit is not None:
        timestamps = timestamps[-limit:]

    results = []
    for ts in timestamps:
        results.append(fetch_one_timeseries_batch(client, series_key, ts))

    logger.info(
        "Fetched %s raw timeseries batches for series '%s'",
        len(results),
        series_key,
    )
    return results
    