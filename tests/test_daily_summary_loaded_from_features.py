import logging

from sqlalchemy import text

from src.load.postgres import get_engine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logger = logging.getLogger(__name__)


def test_daily_summary_loaded_from_features():
    engine = get_engine()

    queries = {
        "count": """
            SELECT COUNT(*) FROM mart.energy_summary_daily
        """,
        "preview": """
            SELECT *
            FROM mart.energy_summary_daily
            ORDER BY date_berlin
        """,
    }

    with engine.connect() as conn:
        count = conn.execute(text(queries["count"])).scalar()
        rows = conn.execute(text(queries["preview"])).fetchall()

    print(f"\nRows in mart.energy_summary_daily: {count}")
    print("\nPreview:")
    for row in rows:
        print(row)


if __name__ == "__main__":
    test_daily_summary_loaded_from_features()