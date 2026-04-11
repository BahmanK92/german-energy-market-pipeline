import logging

from sqlalchemy import text

from src.load.postgres import get_engine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logger = logging.getLogger(__name__)


def test_raw_loaded_data():
    engine = get_engine()

    queries = {
        "total_rows": """
            SELECT COUNT(*) AS n
            FROM raw.smard_timeseries_long
        """,
        "series_counts": """
            SELECT series_key, COUNT(*) AS row_count
            FROM raw.smard_timeseries_long
            GROUP BY series_key
            ORDER BY series_key
        """,
        "batch_counts": """
            SELECT series_key, source_batch_timestamp, COUNT(*) AS row_count
            FROM raw.smard_timeseries_long
            GROUP BY series_key, source_batch_timestamp
            ORDER BY series_key, source_batch_timestamp
        """,
        "preview": """
            SELECT
                series_key,
                value_name,
                datetime_utc,
                datetime_berlin,
                value,
                source_batch_timestamp
            FROM raw.smard_timeseries_long
            ORDER BY series_key, datetime_utc
            LIMIT 20
        """,
    }

    with engine.connect() as conn:
        total_rows = conn.execute(text(queries["total_rows"])).scalar()

        series_counts = conn.execute(text(queries["series_counts"])).fetchall()
        batch_counts = conn.execute(text(queries["batch_counts"])).fetchall()
        preview_rows = conn.execute(text(queries["preview"])).fetchall()

    print(f"\nTotal rows in raw.smard_timeseries_long: {total_rows}")

    print("\nRows by series:")
    for row in series_counts:
        print(f"- {row[0]}: {row[1]}")

    print("\nRows by series and batch timestamp:")
    for row in batch_counts:
        print(f"- {row[0]} | {row[1]} | {row[2]}")

    print("\nPreview rows:")
    for row in preview_rows:
        print(row)


if __name__ == "__main__":
    test_raw_loaded_data()