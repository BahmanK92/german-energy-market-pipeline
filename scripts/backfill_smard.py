import logging
from typing import Iterable, List, Optional
from sqlalchemy import text

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
    "residual_load_smard",
    "solar",
    "wind_onshore",
    "wind_offshore",
    "biomass",
    "hydro",
    "lignite",
    "hard_coal",
    "gas",
    "other_conventional",]


def choose_incremental_batch_timestamps(
    client: SmardClient,
    engine,
    series_key: str,
    limit: Optional[int] = None,
) -> List[int]:
    """
    Return only the complete batch timestamps that are newer than the
    latest already-loaded batch for this series.

    Rules:
    - skip the newest SMARD timestamp because it may still be open/incomplete
    - only return timestamps greater than the latest loaded timestamp
    - if limit is provided, keep only the latest N of those missing timestamps
    """
    timestamps = client.get_available_timestamps(series_key)

    if len(timestamps) < 2:
        raise ValueError(f"Not enough timestamps available for series '{series_key}'")

    complete_timestamps = timestamps[:-1]

    latest_loaded = get_latest_loaded_batch_timestamp(engine=engine, series_key=series_key)

    if latest_loaded is None:
        missing_timestamps = complete_timestamps
    else:
        missing_timestamps = [ts for ts in complete_timestamps if ts > latest_loaded]

    if limit is not None:
        missing_timestamps = missing_timestamps[-limit:]

    logger.info(
        "Series '%s': latest_loaded=%s, complete_available=%s, missing_to_load=%s",
        series_key,
        latest_loaded,
        len(complete_timestamps),
        len(missing_timestamps),
    )

    return missing_timestamps

def get_latest_loaded_batch_timestamp(
    engine,
    series_key: str,
) -> Optional[int]:
    """
    Return the latest source_batch_timestamp already loaded in raw
    for one series_key, or None if nothing is loaded yet.
    """
    query = text(
        """
        SELECT MAX(source_batch_timestamp)
        FROM raw.smard_timeseries_long
        WHERE series_key = :series_key
        """
    )

    with engine.connect() as conn:
        result = conn.execute(query, {"series_key": series_key}).scalar()

    return int(result) if result is not None else None

def backfill_smard(
    series_keys: Optional[Iterable[str]] = None,
    limit_per_series: Optional[int] = None,
) -> None:
    """
    Backfill normalized SMARD batches into raw.smard_timeseries_long.

    Default behavior:
    - load only complete timestamps
    - skip the newest possibly-open timestamp on SMARD
    - load only timestamps newer than what is already present in raw
    - optionally limit the number of missing timestamps per series for development
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
        timestamps = choose_incremental_batch_timestamps(
            client=client,
            engine=engine,
            series_key=series_key,
            limit=limit_per_series,
        )

        if not timestamps:
            logger.info("No new complete timestamps to load for series '%s'", series_key)
            continue

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
    