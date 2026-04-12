import logging

from sqlalchemy import text

from scripts.backfill_smard import backfill_smard
from src.load.postgres import get_engine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logger = logging.getLogger(__name__)


def get_raw_counts(engine):
    queries = {
        "total_rows": "SELECT COUNT(*) FROM raw.smard_timeseries_long",
        "distinct_batches": "SELECT COUNT(DISTINCT (series_key, source_batch_timestamp)) FROM raw.smard_timeseries_long",
    }

    results = {}
    with engine.connect() as conn:
        for key, query in queries.items():
            results[key] = conn.execute(text(query)).scalar()

    return results


def test_backfill_incremental():
    engine = get_engine()

    before = get_raw_counts(engine)

    backfill_smard()

    after_first = get_raw_counts(engine)

    backfill_smard()

    after_second = get_raw_counts(engine)

    assert after_first["total_rows"] >= before["total_rows"], (
        "First backfill should not reduce raw row count"
    )

    assert after_second["total_rows"] == after_first["total_rows"], (
        "Second backfill should not add duplicate rows when nothing new exists"
    )

    assert after_second["distinct_batches"] == after_first["distinct_batches"], (
        "Second backfill should not add duplicate batch loads when nothing new exists"
    )

    print("\nIncremental backfill test passed successfully.")
    print(f"Before: {before}")
    print(f"After first run: {after_first}")
    print(f"After second run: {after_second}")


if __name__ == "__main__":
    test_backfill_incremental()