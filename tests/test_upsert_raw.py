import logging

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


def test_upsert_raw():
    """
    Test raw upsert by:
    1. fetching one SMARD batch
    2. normalizing it
    3. upserting twice (to test idempotency)
    4. checking row count in DB
    """

    engine = get_engine()
    client = SmardClient()

    series_key = "price"

    timestamps = client.get_available_timestamps(series_key)
    test_timestamp = timestamps[-2]  # use last complete batch

    logger.info(
        "Testing raw upsert for series '%s' at timestamp %s",
        series_key,
        test_timestamp,
    )

    raw_batch = fetch_one_timeseries_batch(
        client=client,
        series_key=series_key,
        timestamp=test_timestamp,
    )

    df = normalize_smard_payload(raw_batch)

    logger.info("Upserting first time...")
    upsert_raw_dataframe(df, engine)

    logger.info("Upserting second time (should not duplicate)...")
    upsert_raw_dataframe(df, engine)

    # Check row count
    query = """
    SELECT COUNT(*) 
    FROM raw.smard_timeseries_long
    WHERE series_key = :series_key
    """

    with engine.connect() as conn:
        result = conn.execute(text(query), {"series_key": series_key})
        count = result.scalar()

    print(f"\nRows in raw.smard_timeseries_long for '{series_key}': {count}")
    print("Expected: 168 (not 336) if upsert works correctly")


if __name__ == "__main__":
    test_upsert_raw()