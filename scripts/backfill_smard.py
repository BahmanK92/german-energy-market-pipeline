import logging
from typing import Iterable, List, Optional

from src.clients.smard_client import SmardClient
from src.extract.fetch_timeseries import fetch_one_timeseries_batch
from src.load.postgres import get_engine
from src.load.upsert_raw import upsert_raw_dataframe
from src.transform.normalize_smard_json import normalize_smard_payload

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logger = logging.getLogger(__name__)


DEFAULT_SERIES_KEYS = [
    "price",
    "load",
    "solar",
]


def choose_complete_batch_timestamps(
    client: SmardClient,
    series_key: str,
    limit: int,
) -> List[int]:
    """
    Return the latest N complete batch timestamps for one series.

    We intentionally skip the newest timestamp because it may still be
    incomplete/open on SMARD.
    """
    timestamps = client.get_available_timestamps(series_key)

    if len(timestamps) < 2:
        raise ValueError(f"Not enough timestamps available for series '{series_key}'")

    complete_timestamps = timestamps[:-1]

    if not complete_timestamps:
        raise ValueError(f"No complete timestamps available for series '{series_key}'")

    chosen = complete_timestamps[-limit:]
    logger.info(
        "Chosen %s complete timestamps for series '%s': %s",
        len(chosen),
        series_key,
        chosen,
    )
    return chosen


def backfill_smard(
    series_keys: Optional[Iterable[str]] = None,
    limit_per_series: int = 2,
) -> None:
    """
    Backfill normalized SMARD batches into raw.smard_timeseries_long.

    First controlled version:
    - default to a small set of series
    - default to latest 2 complete timestamps per series
    - normalize and upsert each batch into raw
    """
    client = SmardClient()
    engine = get_engine()

    if series_keys is None:
        series_keys = DEFAULT_SERIES_KEYS

    series_keys = list(series_keys)

    logger.info(
        "Starting SMARD backfill for %s series with limit_per_series=%s",
        len(series_keys),
        limit_per_series,
    )

    total_batches = 0
    total_rows = 0

    for series_key in series_keys:
        timestamps = choose_complete_batch_timestamps(
            client=client,
            series_key=series_key,
            limit=limit_per_series,
        )

        for timestamp in timestamps:
            logger.info(
                "Backfilling series '%s' for timestamp %s",
                series_key,
                timestamp,
            )

            raw_batch = fetch_one_timeseries_batch(
                client=client,
                series_key=series_key,
                timestamp=timestamp,
            )

            normalized_df = normalize_smard_payload(raw_batch)

            upsert_raw_dataframe(normalized_df, engine)

            total_batches += 1
            total_rows += len(normalized_df)

    logger.info(
        "Backfill finished successfully: %s batches processed, %s normalized rows handled",
        total_batches,
        total_rows,
    )


if __name__ == "__main__":
    backfill_smard()
    